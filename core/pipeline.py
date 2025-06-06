# nps_processor/core/pipeline.py

import concurrent.futures
from datetime import datetime
import os
import traceback
import csv
import time # Added for progress timing

# Import from our package modules
from .. import config # Use .. for sibling access from core
from ..utils import helpers
from . import api_client
from . import db_manager
from . import call_processor

def _log_progress_eta(processed_count, total_items, start_time, item_name="items", stage_label="Progress", log_interval=1):
    """
    Helper function to log progress, speed, and ETA for a stage.

    Args:
        processed_count (int): Number of items processed so far.
        total_items (int): Total number of items to process in this stage.
        start_time (float): The time.monotonic() when the stage started.
        item_name (str): Name of the items being processed (e.g., "pages", "calls").
        stage_label (str): A label for the stage being logged.
        log_interval (int): Log every N items. Also logs the first and last item.
    """
    if total_items == 0:
        # This case should ideally be handled by the caller by not calling this func,
        # or by printing a specific "No items to process" message.
        return

    # This is for an initial call before any item is processed.
    if processed_count == 0:
        print(f"  {stage_label}: Starting to process {total_items} {item_name}...")
        return

    # Determine if we should log for the current item
    # Always log if:
    # 1. log_interval is 1 (log every item)
    # 2. It's the very first item processed (processed_count == 1) - gives immediate feedback.
    # 3. It's the very last item (processed_count == total_items) - gives final status.
    # 4. It's an interval item (processed_count % log_interval == 0).
    should_log = (
        log_interval <= 1 or # if log_interval is 0 or 1, log every item
        processed_count == 1 or
        processed_count == total_items or
        (processed_count > 1 and processed_count % log_interval == 0) # Avoid 0 % N for interval
    )

    if not should_log:
        return

    elapsed_time = time.monotonic() - start_time
    progress_percent = (processed_count / total_items) * 100

    if elapsed_time > 0 and processed_count > 0:
        items_per_sec = processed_count / elapsed_time
        remaining_items = total_items - processed_count
        
        if items_per_sec > 0:
            eta_seconds = remaining_items / items_per_sec
            if eta_seconds < 0: eta_seconds = 0 # Can happen due to timing fluctuations
            
            if eta_seconds < 60:
                eta_str = f"{eta_seconds:.1f}s"
            elif eta_seconds < 3600:
                eta_str = f"{eta_seconds/60:.1f}m"
            else:
                eta_str = f"{eta_seconds/3600:.1f}h"
        else: # Should generally not happen if elapsed_time > 0 and processed_count > 0
            eta_str = "N/A"
        
        speed_str = f"{items_per_sec:.2f} {item_name}/s"
    else: # Very fast, or first item where elapsed_time might be ~0
        eta_str = "calculating..."
        speed_str = "calculating..."

    print(f"  {stage_label}: {processed_count}/{total_items} {item_name} ({progress_percent:.1f}%) complete. "
          f"Speed: {speed_str}. ETA: {eta_str}")


def execute_daily_pipeline(
        date_to_process_str,
        output_csv_filename,
        max_pages_for_testing=None
    ):
    """
    Orchestrates the entire pipeline for fetching, processing, and generating NPS calls for a date.
    """
    pipeline_start_time = time.monotonic()
    print(f"\n{'='*10} PIPELINE STARTING FOR DATE: {date_to_process_str} {'='*10}")
    if max_pages_for_testing:
        print(f"--- Pipeline Test Mode: Max {max_pages_for_testing} page(s) ---")

    # --- 0. Initialization ---
    processed_phone_email_pairs_today = set()
    try:
        current_processing_date_obj = datetime.strptime(date_to_process_str, '%Y-%m-%d').date()
    except ValueError:
        print(f"Error: Invalid date string format '{date_to_process_str}'. Please use YYYY-MM-DD.")
        return

    print("--- Pipeline Stage 0: Initializing User Status Map ---")
    user_status_map_start_time = time.monotonic()
    user_status_map = api_client.fetch_user_status_data()
    if not user_status_map:
        print("  Warning: User status map is empty or failed to fetch.")
        user_status_map = {} # Ensure it's a dict to prevent downstream errors
    else:
        print(f"  User status map initialized with {len(user_status_map)} entries. (Took {time.monotonic() - user_status_map_start_time:.2f}s)")


    # --- Stage 1: Fetch all raw CDR data from API ---
    all_raw_call_records = []
    start_datetime_str = f"{date_to_process_str} 00:00:00"
    end_datetime_str = f"{date_to_process_str} 23:59:59"

    print("\n--- Pipeline Stage 1: Fetching CDR API Data ---")
    stage1_total_start_time = time.monotonic()
    _, page1_results, page1_error, page1_pagination = api_client.fetch_cdr_api_page(1, start_datetime_str, end_datetime_str)

    max_api_page = 0
    if page1_error:
        print(f"  Critical Error fetching Page 1: {page1_error}. Aborting pipeline for this date.")
        return
    
    if page1_pagination:
        max_p = page1_pagination.get('max_page', 0)
        rows = page1_pagination.get('row_count', 0)
        p_size = page1_pagination.get('page_size', 100)
        if p_size == 0: p_size = 100 # Default page size if not provided or zero
        print(f"  CDR API Pagination (Page 1): Total Records: {rows}, Max Page (from API): {max_p}, Page Size: {p_size}")
        if max_p > 0:
            max_api_page = max_p
        elif rows > 0 : # Calculate max_page if not directly provided by API but row_count is
            max_api_page = (rows + p_size - 1) // p_size
        elif page1_results: # If rows is 0 but page 1 has results, assume only 1 page
            max_api_page = 1
    else:
        print("  Warning: No pagination info from Page 1. Assuming only 1 page if results exist.")
        if page1_results:
            max_api_page = 1

    if page1_results is None: # Should be caught by page1_error, but double-check
        print(f"  Critical Error: No results from Page 1 for {date_to_process_str}. Aborting pipeline.")
        return
    
    if page1_results:
        all_raw_call_records.extend(page1_results)
    print(f"  Fetched Page 1. Current raw records: {len(all_raw_call_records)}")


    pages_to_fetch_in_parallel = []
    actual_max_pages_to_process = min(max_api_page, max_pages_for_testing) if max_pages_for_testing is not None else max_api_page
    
    if actual_max_pages_to_process > 1: # If there are more pages beyond page 1
        pages_to_fetch_in_parallel = list(range(2, actual_max_pages_to_process + 1))

    if pages_to_fetch_in_parallel:
        total_parallel_pages = len(pages_to_fetch_in_parallel)
        print(f"  Fetching {total_parallel_pages} more pages (Pages 2 to {actual_max_pages_to_process}) in parallel (up to {config.MAX_PAGE_FETCH_WORKERS} workers)...")
        
        processed_api_pages_count = 0
        stage1_parallel_fetch_start_time = time.monotonic()
        _log_progress_eta(0, total_parallel_pages, stage1_parallel_fetch_start_time, item_name="API pages", stage_label="Parallel Fetch")

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_PAGE_FETCH_WORKERS) as p_exec:
            futures = {p_exec.submit(api_client.fetch_cdr_api_page, pg, start_datetime_str, end_datetime_str): pg for pg in pages_to_fetch_in_parallel}
            for fut in concurrent.futures.as_completed(futures):
                pg, res, err, _ = fut.result()
                processed_api_pages_count += 1
                if err:
                    print(f"    Error fetching CDR page {pg}: {err}")
                elif res is not None:
                    all_raw_call_records.extend(res)
                
                _log_progress_eta(
                    processed_api_pages_count,
                    total_parallel_pages,
                    stage1_parallel_fetch_start_time,
                    item_name="API pages",
                    stage_label="Parallel Fetch",
                    log_interval=1 # Log every page as API calls can be slow
                )
        print(f"  Finished parallel page fetching. (Took {time.monotonic() - stage1_parallel_fetch_start_time:.2f}s)")
    
    print(f"Stage 1 (Fetching CDR API Data) complete. Total raw records: {len(all_raw_call_records)}. (Took {time.monotonic() - stage1_total_start_time:.2f}s)")
    if not all_raw_call_records:
        print(f"  No raw records after API fetch for {date_to_process_str}. Ending pipeline for this date.")
        return

    # --- Stage 2: Initial Python Filtering ---
    print("\n--- Pipeline Stage 2: Initial Python Filtering ---")
    stage2_start_time = time.monotonic()
    calls_needing_email_lookup = []
    phones_to_lookup_cleaned = set()
    
    total_raw_for_filter = len(all_raw_call_records)
    if total_raw_for_filter > 0:
        log_interval_stage2 = max(1, total_raw_for_filter // 20 if total_raw_for_filter > 20 else 1)
        _log_progress_eta(0, total_raw_for_filter, stage2_start_time, item_name="raw records", stage_label="Initial Filter")
        for idx, call_item in enumerate(all_raw_call_records):
            if call_processor.initial_call_filter(call_item):
                direction = call_item.get('direction')
                phone_raw = call_item.get('source') if direction == 'IN' else call_item.get('destination')
                cleaned_phone = helpers.clean_phone_for_db_lookup(phone_raw)
                if cleaned_phone:
                    calls_needing_email_lookup.append({'call_data': call_item, 'cleaned_phone': cleaned_phone})
                    phones_to_lookup_cleaned.add(cleaned_phone)
            if (idx + 1) % log_interval_stage2 == 0 or (idx + 1) == total_raw_for_filter:
                 _log_progress_eta(idx + 1, total_raw_for_filter, stage2_start_time, item_name="raw records", stage_label="Initial Filter", log_interval=log_interval_stage2)
    
    print(f"Stage 2 (Initial Python Filtering) complete: {len(calls_needing_email_lookup)} calls pass initial filters, for {len(phones_to_lookup_cleaned)} unique phones. (Took {time.monotonic() - stage2_start_time:.2f}s)")
    if not calls_needing_email_lookup:
        print("  No calls need email lookup after initial filters. Ending pipeline for this date.")
        return

    # --- Stage 3: Batch Email Lookup ---
    print("\n--- Pipeline Stage 3: Batch Email Lookup ---")
    stage3_start_time = time.monotonic()
    master_phone_to_email_map = {}
    phone_list_for_batch_lookup = list(phones_to_lookup_cleaned)
    total_phones_for_lookup = len(phone_list_for_batch_lookup)

    if total_phones_for_lookup > 0:
        num_batches = (total_phones_for_lookup + config.DB_EMAIL_LOOKUP_BATCH_SIZE - 1) // config.DB_EMAIL_LOOKUP_BATCH_SIZE
        print(f"  Preparing to lookup emails for {total_phones_for_lookup} unique phones in {num_batches} batches (batch size: {config.DB_EMAIL_LOOKUP_BATCH_SIZE}).")
        
        processed_batches_count = 0
        _log_progress_eta(0, num_batches, stage3_start_time, item_name="batches", stage_label="Email Lookup")

        for i in range(0, total_phones_for_lookup, config.DB_EMAIL_LOOKUP_BATCH_SIZE):
            batch = phone_list_for_batch_lookup[i:i+config.DB_EMAIL_LOOKUP_BATCH_SIZE]
            master_phone_to_email_map.update(db_manager.batch_fetch_emails_from_db(batch))
            processed_batches_count += 1
            
            _log_progress_eta(
                processed_batches_count,
                num_batches,
                stage3_start_time, # Use stage3_start_time for overall ETA
                item_name="batches",
                stage_label="Email Lookup",
                log_interval=1 # Log every batch
            )
    else:
        print("  No unique phones require email lookup.")

    valid_emails_found = sum(1 for e in master_phone_to_email_map.values() if e and "ERROR" not in str(e).upper() and e != "N/A") # Robust error check
    print(f"Stage 3 (Batch Email Lookup) complete. Found valid emails for {valid_emails_found} of {len(master_phone_to_email_map)} looked-up phones. (Took {time.monotonic() - stage3_start_time:.2f}s)")


    # --- Stage 4: Final Processing (Parallel) ---
    print(f"\n--- Pipeline Stage 4: Final Call Processing (up to {config.MAX_CALL_PROCESS_WORKERS} workers)... ---")
    stage4_start_time = time.monotonic()
    final_eligible_rows = []
    
    tasks_final = []
    for idx, item in enumerate(calls_needing_email_lookup):
        # Construct the argument tuple for process_eligible_call_final
        arg_tuple = (
            item['call_data'],
            item['cleaned_phone'],
            master_phone_to_email_map,
            user_status_map,
            item['call_data'].get('uniqueid') or item['call_data'].get('id', f"FinP{idx}"),
            current_processing_date_obj
        )
        tasks_final.append(arg_tuple)
    
    total_calls_to_finalize = len(tasks_final)
    if total_calls_to_finalize > 0:
        processed_calls_final_count = 0
        log_interval_final_proc = max(1, total_calls_to_finalize // 20 if total_calls_to_finalize > 20 else 1)
        _log_progress_eta(0, total_calls_to_finalize, stage4_start_time, item_name="calls", stage_label="Final Call Processing")

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_CALL_PROCESS_WORKERS) as fin_exec:
            # Create a mapping from future to call_id for logging errors
            # arg[4] is call_id_for_log from the tuple structure
            future_to_call_id = {fin_exec.submit(call_processor.process_eligible_call_final, *arg_tuple): arg_tuple[4] for arg_tuple in tasks_final}
            
            for fut in concurrent.futures.as_completed(future_to_call_id):
                processed_calls_final_count +=1
                call_id_for_error_log = future_to_call_id[fut]
                _log_progress_eta(
                    processed_calls_final_count,
                    total_calls_to_finalize,
                    stage4_start_time,
                    item_name="calls",
                    stage_label="Final Call Processing",
                    log_interval=log_interval_final_proc
                )
                try:
                    csv_row, phone_uniq, email_uniq = fut.result() # This will raise exception if one occurred in the worker
                    if csv_row:
                        # Ensure phone_uniq and email_uniq are valid before adding to set
                        if phone_uniq and email_uniq and isinstance(phone_uniq, str) and isinstance(email_uniq, str):
                             if (phone_uniq, email_uniq) not in processed_phone_email_pairs_today:
                                final_eligible_rows.append(csv_row)
                                processed_phone_email_pairs_today.add((phone_uniq, email_uniq))
                        # else:
                        #     print(f"    Debug: Final processor for call '{call_id_for_error_log}' returned invalid phone/email for uniqueness: P='{phone_uniq}', E='{email_uniq}'")
                except Exception as e:
                    print(f"    Error during final processing for call ID '{call_id_for_error_log}': {e}")
                    # To see the full traceback from the worker thread, you might need to
                    # ensure exceptions are fully captured and reraised or logged more deeply.
                    # traceback.print_exc() # Uncomment for more detailed error from worker
            
            # Ensure all thread-local DB connections are closed if used by call_processor and db_manager
            if hasattr(db_manager, 'close_thread_db_connection'):
                print(f"  Closing up to {min(total_calls_to_finalize, config.MAX_CALL_PROCESS_WORKERS)} thread-local DB connections...")
                # Only submit cleanup tasks if threads were potentially used
                if total_calls_to_finalize > 0 and config.MAX_CALL_PROCESS_WORKERS > 0 :
                    num_cleanup_tasks = min(total_calls_to_finalize, config.MAX_CALL_PROCESS_WORKERS)
                    cleanup_futures = [fin_exec.submit(db_manager.close_thread_db_connection) for _ in range(num_cleanup_tasks)]
                    concurrent.futures.wait(cleanup_futures)
                    print(f"  {len(cleanup_futures)} DB connection cleanup tasks submitted and awaited.")
    else:
        print("  No calls to process for final eligibility.")
        
    print(f"Stage 4 (Final Call Processing) complete. Total unique eligible calls for CSV: {len(final_eligible_rows)}. (Took {time.monotonic() - stage4_start_time:.2f}s)")

    # --- Stage 5: Write to CSV ---
    print(f"\n--- Pipeline Stage 5: Writing to CSV ---")
    stage5_start_time = time.monotonic()
    if final_eligible_rows:
        if not os.path.exists(config.CSV_OUTPUT_DIRECTORY):
            try:
                os.makedirs(config.CSV_OUTPUT_DIRECTORY)
                print(f"  Created CSV output directory: {config.CSV_OUTPUT_DIRECTORY}")
            except OSError as e:
                print(f"  Error creating CSV output directory {config.CSV_OUTPUT_DIRECTORY}: {e}. Aborting CSV write.")
                # Decide if you want to proceed to stage 6 or return
                # For now, let's print error and continue to NPS recording if possible
        
        full_csv_path = os.path.join(config.CSV_OUTPUT_DIRECTORY, output_csv_filename)
        print(f"  Writing {len(final_eligible_rows)} calls to {full_csv_path}...")
        try:
            with open(full_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=config.CSV_HEADERS)
                writer.writeheader()
                writer.writerows(final_eligible_rows)
            print(f"  Successfully wrote to {full_csv_path}")
        except IOError as e:
            print(f"  Error writing CSV to {full_csv_path}: {e}")
    else:
        print(f"  No unique eligible calls for {date_to_process_str} to write to CSV.")
    print(f"Stage 5 (Writing to CSV) complete. (Took {time.monotonic() - stage5_start_time:.2f}s)")


    # --- Stage 6: Record NPS Selections ---
    print(f"\n--- Pipeline Stage 6: Recording NPS Selections in DB ---")
    stage6_start_time = time.monotonic()
    if final_eligible_rows:
        total_nps_to_record = len(final_eligible_rows)
        print(f"  Recording {total_nps_to_record} NPS selections...")
        db_conn_rec = db_manager.create_db_connection_for_single_use()
        if db_conn_rec:
            recorded_nps_count = 0
            log_interval_nps_record = max(1, total_nps_to_record // 20 if total_nps_to_record > 20 else 1)
            _log_progress_eta(0, total_nps_to_record, stage6_start_time, item_name="NPS selections", stage_label="DB Record")

            try:
                for idx, row in enumerate(final_eligible_rows):
                    phone_cleaned = helpers.clean_phone_for_db_lookup(row.get('Customer pho')) # Use .get for safety
                    email_for_nps = row.get('Customer Email')
                    interaction_ref = row.get('Interaction reference')
                    row_date = row.get('Date')
                    row_hour = row.get('Hour')

                    if phone_cleaned and email_for_nps and interaction_ref: # Ensure essential data is present
                        call_dt_str = f"{row_date} {row_hour}" if row_date != "N/A" and row_hour != "N/A" else None
                        if db_manager.record_nps_selection(db_conn_rec, phone_cleaned, email_for_nps,
                                                           date_to_process_str, interaction_ref, call_dt_str):
                            recorded_nps_count +=1
                    
                    _log_progress_eta(
                        idx + 1,
                        total_nps_to_record,
                        stage6_start_time,
                        item_name="NPS selections",
                        stage_label="DB Record",
                        log_interval=log_interval_nps_record
                    )
                print(f"  Successfully recorded/updated {recorded_nps_count} of {total_nps_to_record} NPS selections.")
            except Exception as e:
                print(f"  Error during batch NPS selection recording: {e}")
                traceback.print_exc()
            finally:
                db_conn_rec.close()
                print("  DB connection for NPS recording closed.")
        else:
            print("  Error: Could not connect to DB to record NPS selections.")
    else:
        print(f"  No NPS selections to record for {date_to_process_str}.")
    print(f"Stage 6 (Recording NPS Selections) complete. (Took {time.monotonic() - stage6_start_time:.2f}s)")
    
    pipeline_duration = time.monotonic() - pipeline_start_time
    print(f"\n{'='*10} PIPELINE FINISHED FOR DATE: {date_to_process_str} {'='*10}")
    print(f"Total pipeline execution time for {date_to_process_str}: {pipeline_duration:.2f} seconds.")