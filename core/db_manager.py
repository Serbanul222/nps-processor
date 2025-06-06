# nps_processor/core/db_manager.py

import mariadb
import threading
import traceback

# Import from config
try:
    from .. import config
except ImportError:
    import sys
    sys.path.append('..')
    import config

# Thread-local storage for database connections for call processing workers
thread_local_db = threading.local()

def get_thread_db_connection():
    """
    Gets or creates a DB connection for the current worker thread using thread-local storage.
    This connection is intended to be reused by a thread for multiple operations if needed.
    """
    if not hasattr(thread_local_db, "connection") or thread_local_db.connection is None:
        # print(f"Thread {threading.get_ident()}: No existing thread-local DB connection, creating new one.") # Debug
        try:
            conn = mariadb.connect(
                user=config.DB_USER, password=config.DB_PASSWORD, host=config.DB_HOST,
                port=config.DB_PORT, database=config.DB_NAME, connect_timeout=10
            )
            conn.ping() # Test connection
            thread_local_db.connection = conn
            # print(f"Thread {threading.get_ident()}: New thread-local DB connection established.") # Debug
        except mariadb.Error as e:
            print(f"Thread {threading.get_ident()}: Failed to create thread-local DB connection: {e}")
            thread_local_db.connection = None # Ensure it's None on failure
            return None # Explicitly return None on failure
        
    # If connection exists, try to ping to ensure it's alive
    try:
        if thread_local_db.connection: # Check if it was set (even if to None by a previous error)
            thread_local_db.connection.ping()
            # print(f"Thread {threading.get_ident()}: Existing thread-local DB connection is alive.") # Debug
        else: # Was None from the start or set to None due to creation failure
             raise mariadb.Error("Connection was None initially.")
    except mariadb.Error: # Ping failed or connection was None
        # print(f"Thread {threading.get_ident()}: Thread-local DB connection ping failed or was None, recreating.") # Debug
        try:
            if hasattr(thread_local_db, "connection") and thread_local_db.connection:
                thread_local_db.connection.close()
        except mariadb.Error:
            pass # Ignore errors closing a broken connection
        
        try:
            conn = mariadb.connect(
                user=config.DB_USER, password=config.DB_PASSWORD, host=config.DB_HOST,
                port=config.DB_PORT, database=config.DB_NAME, connect_timeout=10
            )
            conn.ping()
            thread_local_db.connection = conn
            # print(f"Thread {threading.get_ident()}: Thread-local DB connection recreated.") # Debug
        except mariadb.Error as e:
            print(f"Thread {threading.get_ident()}: Failed to recreate thread-local DB connection: {e}")
            thread_local_db.connection = None
            return None # Explicitly return None on failure
            
    return thread_local_db.connection

def close_thread_db_connection():
    """Closes the DB connection for the current worker thread, if it exists."""
    if hasattr(thread_local_db, "connection") and thread_local_db.connection is not None:
        # print(f"Thread {threading.get_ident()}: Closing thread-local DB connection.") # Debug
        try:
            thread_local_db.connection.close()
        except mariadb.Error as e:
            print(f"Thread {threading.get_ident()}: Error closing DB connection: {e}")
        finally: # Ensure it's set to None even if close fails
            thread_local_db.connection = None

def create_db_connection_for_single_use():
    """Creates a new DB connection, intended for one-off operations like batch lookups."""
    try:
        # print("Creating new single-use DB connection.") # Debug
        conn = mariadb.connect(
            user=config.DB_USER, password=config.DB_PASSWORD, host=config.DB_HOST,
            port=config.DB_PORT, database=config.DB_NAME, connect_timeout=10
        )
        conn.ping(); return conn
    except mariadb.Error as e:
        print(f"Error creating single-use DB connection: {e}"); return None

def batch_fetch_emails_from_db(phone_numbers_to_lookup_cleaned):
    """
    Fetches emails for a batch of *already cleaned* phone numbers.
    Uses a new connection for this batch operation.
    Returns a dictionary: {cleaned_phone_from_db: email}
    """
    phone_to_email_map = {}
    if not phone_numbers_to_lookup_cleaned:
        return phone_to_email_map

    unique_phones_for_batch = list(set(phone_numbers_to_lookup_cleaned)) # Remove duplicates for query
    # print(f"Debug: Batch DB lookup for {len(unique_phones_for_batch)} unique phones.") # Debug

    db_conn = None
    try:
        db_conn = create_db_connection_for_single_use()
        if not db_conn:
            print("Error: Could not establish DB connection for batch email lookup.")
            # Mark all phones in this batch with an error if connection failed
            return {phone: "DB_CONN_ERROR_BATCH" for phone in unique_phones_for_batch}

        cursor = db_conn.cursor()
        
        placeholders = ', '.join(['%s'] * len(unique_phones_for_batch))
        query_simplified = f"""
            SELECT DISTINCT REPLACE(TRIM(telefon), ',', '') as db_phone_matched, email 
            FROM date_contact_clienti 
            WHERE REPLACE(TRIM(telefon), ',', '') IN ({placeholders})
        """
        params = tuple(unique_phones_for_batch)
        cursor.execute(query_simplified, params)
        
        for db_phone_matched, email_val in cursor:
            if email_val: # Only store if email is not null/empty
                phone_to_email_map[db_phone_matched] = email_val
        
        # For phones not found (e.g. 407... not found), try looking them up without '40'
        phones_needing_prefix_check = [
            p for p in unique_phones_for_batch 
            if p.startswith('40') and p not in phone_to_email_map
        ]
        if phones_needing_prefix_check:
            phones_without_prefix = [p[2:] for p in phones_needing_prefix_check]
            if phones_without_prefix: # Ensure list is not empty
                placeholders_no_prefix = ', '.join(['%s'] * len(phones_without_prefix))
                query_no_prefix = f"""
                    SELECT DISTINCT REPLACE(TRIM(telefon), ',', '') as db_phone_matched, email 
                    FROM date_contact_clienti 
                    WHERE REPLACE(TRIM(telefon), ',', '') IN ({placeholders_no_prefix})
                """
                params_no_prefix = tuple(phones_without_prefix)
                cursor.execute(query_no_prefix, params_no_prefix)
                for db_phone_matched_no_prefix, email_val_no_prefix in cursor:
                    if email_val_no_prefix:
                        original_phone_with_prefix = '40' + db_phone_matched_no_prefix
                        if original_phone_with_prefix in phones_needing_prefix_check:
                             phone_to_email_map[original_phone_with_prefix] = email_val_no_prefix
        cursor.close()

    except mariadb.Error as e:
        print(f"Error during batch email DB lookup: {e}")
        for phone in unique_phones_for_batch:
            if phone not in phone_to_email_map:
                phone_to_email_map[phone] = "DB_QUERY_ERROR_BATCH"
    finally:
        if db_conn:
            db_conn.close()
            
    return phone_to_email_map

def get_last_nps_selection_date(db_conn, cleaned_phone_number):
    """
    Queries the nps_send_history table for the last selection date for a contact.
    Returns a date object or None.
    """
    if not db_conn or not cleaned_phone_number:
        return None
    
    last_selection_date = None
    try:
        cursor = db_conn.cursor()
        query = """
            SELECT last_nps_selection_date 
            FROM nps_send_history 
            WHERE cleaned_phone_number = ? 
            ORDER BY last_nps_selection_date DESC 
            LIMIT 1
        """
        cursor.execute(query, (cleaned_phone_number,))
        result = cursor.fetchone()
        if result and result[0]:
            last_selection_date = result[0] # result[0] should be a datetime.date object
        cursor.close()
    except mariadb.Error as e:
        print(f"Thread {threading.get_ident()}: Error getting last NPS date for {cleaned_phone_number}: {e}")
        # In case of query error, assume no prior send to be safe, or handle differently
    return last_selection_date

def record_nps_selection(db_conn, cleaned_phone_number, email_address, 
                         selection_date_str, call_uniqueid, call_datetime_str):
    """
    Records or updates an NPS selection in the nps_send_history table.
    selection_date_str should be 'YYYY-MM-DD'.
    call_datetime_str should be 'YYYY-MM-DD HH:MM:SS'.
    """
    if not db_conn or not cleaned_phone_number or not selection_date_str:
        print("Error: Missing required data for recording NPS selection.")
        return False
    
    try:
        cursor = db_conn.cursor()
        # Using ON DUPLICATE KEY UPDATE requires cleaned_phone_number to be a UNIQUE key
        query = """
            INSERT INTO nps_send_history 
                (cleaned_phone_number, email_address, last_nps_selection_date, 
                 triggering_call_uniqueid, triggering_call_datetime)
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                email_address = VALUES(email_address),
                last_nps_selection_date = VALUES(last_nps_selection_date),
                triggering_call_uniqueid = VALUES(triggering_call_uniqueid),
                triggering_call_datetime = VALUES(triggering_call_datetime),
                script_run_datetime = CURRENT_TIMESTAMP 
        """
        # Ensure dates are in correct format for DB
        # selection_date_obj = datetime.strptime(selection_date_str, '%Y-%m-%d').date()
        # call_datetime_obj = datetime.strptime(call_datetime_str, '%Y-%m-%d %H:%M:%S') if call_datetime_str else None
        
        params = (
            cleaned_phone_number,
            email_address if email_address and "ERROR" not in email_address else None, # Store NULL if email was error
            selection_date_str, # MariaDB can often parse 'YYYY-MM-DD' string for DATE
            call_uniqueid,
            call_datetime_str if call_datetime_str and "ERROR" not in call_datetime_str else None # Store NULL if time parse error
        )
        cursor.execute(query, params)
        db_conn.commit()
        cursor.close()
        # print(f"Debug: Recorded/Updated NPS selection for {cleaned_phone_number} on {selection_date_str}") # Debug
        return True
    except mariadb.Error as e:
        print(f"Error recording NPS selection for {cleaned_phone_number}: {e}")
        try:
            db_conn.rollback() # Rollback on error
        except mariadb.Error as re:
            print(f"Error during rollback: {re}")
        return False
    except ValueError as ve: # For date parsing errors, though direct string passing is often fine
        print(f"Date formatting error for NPS recording of {cleaned_phone_number}: {ve}")
        return False


# Example of how this module might be tested directly (optional)
# nps_processor/core/db_manager.py
# ... (all the functions from the previous version of db_manager.py up to this point) ...

# Example of how this module might be tested directly (optional)
if __name__ == '__main__':
    print("Testing db_manager.py...")

    # Import necessary for testing if not already at top level of this specific test block
    from datetime import datetime, timedelta, date as py_date # Ensure these are available for the test
    import json # For pretty printing email_map

    # Test thread-local connection
    print("\n--- Testing Thread-Local DB Connection ---")
    conn1 = get_thread_db_connection()
    if conn1:
        print(f"Thread {threading.get_ident()} got conn1: {type(conn1)}")
        conn1_again = get_thread_db_connection() # Should be the same object
        print(f"Thread {threading.get_ident()} got conn1_again: {type(conn1_again)}, Same object: {conn1 is conn1_again}")
    else:
        print(f"Thread {threading.get_ident()} failed to get conn1.")
    
    results_from_other_thread = []
    def db_op_in_thread():
        # conn1 from the main thread's scope won't be accessible here directly
        # unless passed or made global, but the point is to test thread-locality.
        # We'll just check if this thread gets its own connection.
        current_main_thread_conn_id = id(conn1) if conn1 else None

        conn_other = get_thread_db_connection()
        if conn_other:
            results_from_other_thread.append(f"Other thread {threading.get_ident()} got conn_other: {type(conn_other)}")
            if current_main_thread_conn_id:
                 results_from_other_thread.append(f"Is conn_other same as main thread's conn1? {id(conn_other) == current_main_thread_conn_id}")
            else:
                 results_from_other_thread.append("Main thread's conn1 was None, cannot compare.")
            close_thread_db_connection() 
            results_from_other_thread.append(f"Other thread closed its connection. Connection object on thread_local_db: {getattr(thread_local_db, 'connection', 'Not Set')}")
        else:
            results_from_other_thread.append(f"Other thread {threading.get_ident()} failed to get conn_other.")

    other_thread = threading.Thread(target=db_op_in_thread)
    other_thread.start()
    other_thread.join()
    for res_item in results_from_other_thread: print(res_item)
    
    conn1_after_other = get_thread_db_connection()
    if conn1_after_other:
        print(f"Main thread {threading.get_ident()} got conn1_after_other: {type(conn1_after_other)}, Same as original conn1: {conn1_after_other is conn1 if conn1 else 'conn1_was_None'}")
    close_thread_db_connection()
    print(f"Main thread closed its connection. Connection object on thread_local_db: {getattr(thread_local_db, 'connection', 'Not Set')}")


    # Test Batch Email Fetch
    print("\n--- Testing Batch Email Fetch ---")
    sample_phones_cleaned = ["40712345678", "40787654321", "40312345678", "111000111", "40750664142"] # Added a known phone
    email_map = batch_fetch_emails_from_db(sample_phones_cleaned)
    print(f"Email map for sample phones: {json.dumps(email_map, indent=2)}")

    # Test NPS History functions (requires the table to exist)
    print("\n--- Testing NPS History Functions ---")
    test_phone = "40799999999" 
    test_call_id = "test.call.uniqueid.123"
    # Use the imported 'datetime' class directly
    test_call_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # CORRECTED
    # Use the imported 'py_date' alias for date.today()
    today_str = py_date.today().strftime('%Y-%m-%d') # CORRECTED

    test_hist_conn = create_db_connection_for_single_use()
    if test_hist_conn:
        try:
            print(f"Recording NPS selection for {test_phone}...")
            success = record_nps_selection(test_hist_conn, test_phone, "test@example.com", today_str, test_call_id, test_call_time)
            print(f"Recording successful: {success}")

            print(f"Getting last NPS selection date for {test_phone}...")
            last_date = get_last_nps_selection_date(test_hist_conn, test_phone)
            print(f"Last selection date: {last_date} (Type: {type(last_date)})")
            if last_date:
                # Use py_date.today() for current date
                days_diff = (py_date.today() - last_date).days
                print(f"Days since last selection: {days_diff}")
                print(f"Is cooldown period ({config.NPS_COOLDOWN_DAYS} days) still active? {days_diff < config.NPS_COOLDOWN_DAYS}")
        finally:
            test_hist_conn.close()
            print("Test history DB connection closed.")
    else:
        print("Could not connect to DB for NPS history tests.")