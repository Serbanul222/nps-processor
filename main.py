# nps_processor/main.py

from datetime import datetime, timedelta, date as py_date
import os
import sys # For environment variable or argument parsing later

# Import from our package modules
from . import config # Use . for current package
from .core import pipeline # Import the new pipeline module

def main():
    """
    Main entry point for the NPS Processor application.
    Determines the date(s) to process and calls the pipeline.
    """
    # --- Determine processing mode and dates ---
    # For now, using config.py settings. Later, can add argparse or env vars.
    
    run_mode = os.getenv("NPS_PROCESSOR_MODE", "DEFAULT") # Example: Read from env var
    
    dates_to_process = []
    max_pages_for_testing_run = None

    if run_mode.upper() == "TEST" or config.TEST_DATES_OVERRIDE:
        dates_to_process = config.TEST_DATES_OVERRIDE if config.TEST_DATES_OVERRIDE else \
                           [(py_date.today() - timedelta(days=1)).strftime('%Y-%m-%d')] # Default test to yesterday
        max_pages_for_testing_run = config.TEST_MAX_PAGES_PER_DATE
        print(f"--- MAIN: RUNNING IN TEST MODE ---")
        if max_pages_for_testing_run is not None:
             print(f"--- Fetching a maximum of {max_pages_for_testing_run} page(s) per date ---")
    else: # Production mode
        yesterday_dt = py_date.today() - timedelta(days=1)
        dates_to_process = [yesterday_dt.strftime('%Y-%m-%d')]
        max_pages_for_testing_run = None # Process all pages
        print(f"--- MAIN: RUNNING IN PRODUCTION MODE ---")
    
    print(f"--- Dates to process: {', '.join(dates_to_process)} ---")

    overall_start_time = datetime.now()

    for specific_date_str in dates_to_process:
        # Filename construction remains here as it's tied to the loop
        csv_filename = f"{config.CSV_FILENAME_PREFIX}{specific_date_str}"
        if max_pages_for_testing_run is not None:
            csv_filename += f"_TEST_{max_pages_for_testing_run}PAGE"
        csv_filename += ".csv"
        
        # The set for tracking unique phone-email pairs per day is managed within the pipeline now.
        # If it needed to persist across calls to execute_daily_pipeline for the *same day* (e.g. script restart),
        # then it would need to be managed here or loaded/saved.
        # But for a clean run per day, pipeline.py initializing it is fine.
        
        pipeline.execute_daily_pipeline(
            specific_date_str,
            csv_filename, # Pass the fully constructed filename
            max_pages_for_testing=max_pages_for_testing_run
        )

    overall_end_time = datetime.now()
    
    print("\n" + "="*70)
    print("ALL SPECIFIED DATES PROCESSED.")
    print(f"Total execution time for all dates: {overall_end_time - overall_start_time}")
    # ... (rest of your summary print, can also be moved to a summary function)
    print(f"Data fetched for dates: {', '.join(dates_to_process)}")
    print("Summary of criteria for CSV export (applied per call):")
    print(f"1. 'code' field (from API) IN {config.VALID_CALL_CODES_FOR_NPS} (case-insensitive)")
    print(f"2. Bill duration >= {config.MIN_BILL_DURATION_SECONDS} sec")
    print("3. NO Transfers")
    print(f"4. Email found (not 'N/A'/error/empty) AND unique (Phone, Email) pair for the processed day AND NPS cooldown ({config.NPS_COOLDOWN_DAYS} days) respected")
    print("5. Employee Reference from /api/users-status (extension) or CDR (user_id/user_fullname)")
    print("6. Customer email looked up in 'magazia_de_date.date_contact_clienti' using 'telefon' column")
    print("="*70)

if __name__ == "__main__":
    # Ensure 're' module is available (though imported at top of other files, good for __main__)
    try: import re 
    except ImportError: print("Critical error: 're' module could not be imported."); exit(1)
    
    main() # Call the new main function