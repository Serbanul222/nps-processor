# nps_processor/core/call_processor.py

from datetime import datetime, timedelta # For 7-day rule calculation
import traceback 
# Import from other modules in the package
try:
    from .. import config
    from ..utils import helpers
    from . import db_manager # To use get_last_nps_selection_date
except ImportError:
    # Fallback for direct execution or different project structure
    import sys
    sys.path.append('..') # Add parent directory
    sys.path.append('.')  # Add current directory for core if core is also a sibling
    import config
    from utils import helpers
    # If db_manager is in the same 'core' directory when running directly:
    if 'db_manager' not in sys.modules: # Avoid re-importing if already available
         import db_manager


def process_eligible_call_final(
        call_data, 
        cleaned_customer_phone, 
        phone_to_email_map, 
        user_status_map, 
        call_id_for_log,
        processing_date_obj 
    ):
    """
    Final processing for a call that has passed initial filters and has its email looked up.
    This function applies the 7-day NPS cooldown rule and constructs the final CSV row.

    Args:
        call_data (dict): The raw CDR data for the call.
        cleaned_customer_phone (str): The cleaned phone number used for email lookup.
        phone_to_email_map (dict): Map of {cleaned_phone: email} from batch DB lookup.
        user_status_map (dict): Map of {username_key: extension/user_id} from user status API.
        call_id_for_log (str): A unique identifier for logging (e.g., uniqueid or CDR id).
        processing_date_obj (datetime.date): The date for which records are being processed.

    Returns:
        tuple: (csv_row_dict, customer_phone_for_uniqueness, customer_email_for_uniqueness) 
               or (None, None, None) if the call is finally deemed ineligible.
    """
    ineligible_return = (None, None, None)

    # 1. Get Email from the pre-fetched map
    customer_email = phone_to_email_map.get(cleaned_customer_phone, "N/A")

    # 2. Filter: Exclude if email is "N/A", an error string, or empty
    #    (This was part of the previous process_call_for_csv_worker, ensuring it here)
    non_valid_email_indicators = ["N/A", "DB_CONN_ERROR_BATCH", "DB_QUERY_ERROR_BATCH"] # Errors from batch lookup
    if not customer_email or customer_email in non_valid_email_indicators:
        # print(f"Debug (final_processor): Call {call_id_for_log} for phone {cleaned_customer_phone} excluded due to email status: {customer_email}")
        return ineligible_return

    # 3. 7-Day NPS Cooldown Check (Requires DB interaction)
    #    This worker uses a thread-local DB connection managed by db_manager.get_thread_db_connection()
    db_conn_for_nps_check = None
    try:
        db_conn_for_nps_check = db_manager.get_thread_db_connection()
        if not db_conn_for_nps_check:
            print(f"Call {call_id_for_log}: DB connection error for 7-day check. Skipping call.")
            return ineligible_return

        last_selection_date_obj = db_manager.get_last_nps_selection_date(db_conn_for_nps_check, cleaned_customer_phone)
        
        if last_selection_date_obj:
            # processing_date_obj is the date for which we are running the script (e.g., yesterday)
            # We want to ensure at least NPS_COOLDOWN_DAYS have passed *before* this processing_date_obj
            # So, if last_selection_date is too recent relative to processing_date_obj, skip.
            # Example: Cooldown 7 days. Processing for May 10th. Last sent May 4th. (10-4=6 days). 6 < 7. Skip.
            # Last sent May 3rd. (10-3=7 days). 7 is not < 7. Eligible.
            if (processing_date_obj - last_selection_date_obj) < timedelta(days=config.NPS_COOLDOWN_DAYS):
                # print(f"Debug (final_processor): Call {call_id_for_log} for phone {cleaned_customer_phone} skipped due to 7-day rule. Last sent: {last_selection_date_obj}, Processing for: {processing_date_obj}")
                return ineligible_return
    except Exception as e:
        print(f"Call {call_id_for_log}: Error during 7-day check for {cleaned_customer_phone}: {e}")
        traceback.print_exc() # Print full traceback for unexpected errors here
        return ineligible_return
    # The thread-local DB connection is not closed here; it's managed by the thread pool lifecycle (or explicit cleanup)

    # --- If all checks pass, construct the CSV row ---
    employee_reference = "N/A"
    direction = call_data.get('direction', "N/A")
    
    cdr_user_id_field = call_data.get('user_id') # Check for 'user_id' in CDR
    if cdr_user_id_field:
        employee_reference = cdr_user_id_field
    else:
        username_key_cdr = None
        channel_to_check_for_user = None
        if direction == 'IN':
            channel_to_check_for_user = call_data.get('dstchannel_formatted')
        elif direction == 'OUT':
            channel_to_check_for_user = call_data.get('channel_formatted')
        
        if channel_to_check_for_user:
            username_key_cdr = helpers.extract_username_from_channel(channel_to_check_for_user)
        
        if username_key_cdr and user_status_map and username_key_cdr in user_status_map:
            employee_reference = user_status_map[username_key_cdr] # Get extension
        elif call_data.get('user_fullname'): # Fallback to fullname if no mapping
            employee_reference = call_data.get('user_fullname')

    call_date_str, call_hour_str = helpers.parse_call_time(call_data.get('time'))
    user_fullname_for_name = call_data.get('user_fullname', "N/A")
    bill_duration_str_csv = call_data.get('bill_duration', '00:00:00')
    
    # Get the original phone number from the call_data for the CSV, not the cleaned one
    customer_phone_raw_from_cdr_csv = "N/A"
    if direction == 'IN':
        customer_phone_raw_from_cdr_csv = call_data.get('source')
    elif direction == 'OUT':
        customer_phone_raw_from_cdr_csv = call_data.get('destination')

    csv_row = {
        'Interaction reference': call_data.get('uniqueid', "N/A"),
        'Date': call_date_str, 
        'Hour': call_hour_str,
        'Employee reference': employee_reference,
        'Employee first name': helpers.get_employee_first_name(user_fullname_for_name),
        'Direction': direction,
        'Customer pho': customer_phone_raw_from_cdr_csv, # Original phone from CDR
        'Destination': call_data.get('destination', "N/A"),
        'Caller ID': call_data.get('caller_id', "N/A"),
        'Customer referen': call_data.get('customer_id') or call_data.get('external_id', "N/A"),
        'Queue name': call_data.get('qname', "N/A"), # This might be the 'Account Code' from UI
        'Call duration': call_data.get('duration', "N/A"),
        'Bill duratio': bill_duration_str_csv,
        'Hold time': call_data.get('hold_time', "N/A"),
        'Customer Email': customer_email
    }
    # Return the CSV row, and the cleaned phone/email for the daily uniqueness check in the main loop
    return csv_row, cleaned_customer_phone, customer_email


# This function applies the initial, non-DB filters.
def initial_call_filter(call_data):
    """
    Applies initial filters that do not require database lookups.
    (code, bill_duration, transfers)
    Returns True if call passes, False otherwise.
    """
    # 1. Filter by 'code' field
    code_field_value = call_data.get('code')
    if not (code_field_value and code_field_value.lower() in config.VALID_CALL_CODES_FOR_NPS):
        return False

    # 2. Check Bill Duration
    bill_duration_str = call_data.get('bill_duration', '00:00:00')
    bill_eligible = False
    if bill_duration_str and bill_duration_str != '00:00:00':
        try:
            parts = bill_duration_str.split(':')
            if len(parts) == 3:
                total_seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                if total_seconds >= config.MIN_BILL_DURATION_SECONDS:
                    bill_eligible = True
        except (ValueError, TypeError):
            pass # bill_eligible remains False
    if not bill_eligible:
        return False

    # 3. Check Transfers
    if bool(call_data.get('transfers')): # If transfers field has any content, it's a transfer
        return False
        
    return True # Passed all initial filters

# Example of how this module might be tested directly (optional, more complex to set up mocks)
if __name__ == '__main__':
    print("Testing call_processor.py...")
    # To test process_eligible_call_final effectively, you'd need to mock:
    # - config
    # - utils.helpers
    # - core.db_manager (especially get_thread_db_connection and get_last_nps_selection_date)
    # - Sample call_data, phone_to_email_map, user_status_map

    # Example minimal test for initial_call_filter
    class MockConfig:
        VALID_CALL_CODES_FOR_NPS = ['incoming', 'outbound']
        MIN_BILL_DURATION_SECONDS = 30

    config = MockConfig() # Override config for this test scope

    sample_call_pass = {
        'code': 'incoming',
        'bill_duration': '00:01:00', # 60 seconds
        'transfers': None 
    }
    sample_call_fail_code = {
        'code': 'internal', # Invalid code
        'bill_duration': '00:01:00',
        'transfers': None
    }
    sample_call_fail_duration = {
        'code': 'outbound',
        'bill_duration': '00:00:10', # Too short
        'transfers': None
    }
    sample_call_fail_transfer = {
        'code': 'incoming',
        'bill_duration': '00:01:00',
        'transfers': "some transfer data" # Has transfers
    }

    print(f"Test call (should pass initial): {initial_call_filter(sample_call_pass)}")
    print(f"Test call (fail code): {initial_call_filter(sample_call_fail_code)}")
    print(f"Test call (fail duration): {initial_call_filter(sample_call_fail_duration)}")
    print(f"Test call (fail transfer): {initial_call_filter(sample_call_fail_transfer)}")

    # Testing process_eligible_call_final would require more setup for dependencies
    print("\nTo test process_eligible_call_final, run the main.py script with test data.")