# nps_processor/core/api_client.py

import requests
import hashlib
import urllib.parse
import traceback
import threading # For USER_STATUS_CACHE_LOCK
import json

# Import from config (assuming config.py is in the parent directory or PYTHONPATH is set)
# For direct execution/testing of this file, you might need to adjust path or pass config
try:
    from .. import config # For package execution (e.g., from main.py)
except ImportError:
    import sys # Fallback for direct execution or if structure is different
    sys.path.append('..') # Add parent directory to path
    import config


# Global Cache for User Status Data (defined here as it's API related)
USER_STATUS_DATA_CACHE = None
USER_STATUS_CACHE_LOCK = threading.Lock()


def _generate_api_hash(params_for_hash_dict, api_key):
    """
    Generates an MD5 hash for API requests based on a dictionary of parameters.
    The dictionary keys should be in the format the API expects for hash generation
    (e.g., 'filters[date_between][0]').
    """
    # Sort parameters by key for consistent hash generation, as some APIs require this.
    # The example PHP http_build_query sorts by key.
    # urllib.parse.urlencode also sorts by default if items are passed as a dict.
    query_string = urllib.parse.urlencode(params_for_hash_dict)
    hash_input = query_string + api_key
    return hashlib.md5(hash_input.encode('utf-8')).hexdigest()


def fetch_user_status_data():
    """
    Fetches user status data from the /api/users-status endpoint and caches it.
    Returns a dictionary mapping username_key to extension/user_id.
    """
    global USER_STATUS_DATA_CACHE
    with USER_STATUS_CACHE_LOCK: # Ensure thread-safe access to the cache
        if USER_STATUS_DATA_CACHE is not None:
            # print("Debug: Using cached user status data.")
            return USER_STATUS_DATA_CACHE

        print("Fetching user status data from API (/api/users-status)...")
        
        # Parameters exactly as needed for hash generation for users-status API
        params_for_hash = {'type': 'sip'} 
        
        api_hash_val = _generate_api_hash(params_for_hash, config.API_KEY)

        # Payload for the POST request (form-urlencoded)
        payload_data_form = {
            'type': 'sip',
            'api_hash': api_hash_val
        }
        headers = {
            "Authorization": f"Bearer {config.API_TOKEN}",
            "Content-Type": "application/x-www-form-urlencoded" # As implied by PHP http_build_query
        }
        
        user_map = {}
        try:
            response = requests.post(
                config.PBX_URL_USERS,
                data=payload_data_form, # Send as form data
                headers=headers,
                timeout=config.REQUEST_TIMEOUT
            )
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            if data.get('has_error', False):
                print(f"API Error fetching user status: {data.get('messages', [])}")
                USER_STATUS_DATA_CACHE = {} # Cache empty on error to prevent repeated failed fetches
                return USER_STATUS_DATA_CACHE # Return empty dict
            
            api_results = data.get('results', {}) # 'results' is a dict for users-status
            if isinstance(api_results, dict):
                for username_key, user_details in api_results.items():
                    if isinstance(user_details, dict) and 'extension' in user_details:
                        user_map[username_key] = user_details['extension']
            
            USER_STATUS_DATA_CACHE = user_map
            print(f"Successfully fetched and cached {len(user_map)} user statuses.")
            return USER_STATUS_DATA_CACHE
        except Exception as e:
            print(f"Failed to fetch user status data: {e}")
            traceback.print_exc()
            USER_STATUS_DATA_CACHE = {} # Cache empty on error
            return USER_STATUS_DATA_CACHE


def fetch_cdr_api_page(page_number, start_datetime_str, end_datetime_str):
    """
    Fetches a single page of CDR data from the /api/cdr endpoint.
    Returns a tuple: (page_number, list_of_call_results_or_None, error_message_or_None, pagination_dict_or_None)
    """
    # print(f"Thread {threading.get_ident()}: Fetching CDR API page {page_number}...") # Debug
    
    # Parameters for hash generation must be flat
    flat_params_for_hash_cdr = {
        'page': page_number,
        'filters[date_between][0]': start_datetime_str,
        'filters[date_between][1]': end_datetime_str
        # Add other static API filters here if they are consistent across all page fetches
        # e.g., 'filters[direction]': 'IN'
    }
    
    api_hash_val_cdr = _generate_api_hash(flat_params_for_hash_cdr, config.API_KEY)
    
    # Payload for the POST request (structured JSON)
    request_payload_body_cdr = {
        'page': page_number,
        'filters': { # Filters are nested in the payload, but flat for hash
            'date_between': [start_datetime_str, end_datetime_str]
            # 'direction': 'IN', # If used in hash, must be here too
        },
        'api_hash': api_hash_val_cdr
    }
    headers_cdr = {
        "Authorization": f"Bearer {config.API_TOKEN}",
        "Content-Type": "application/json" # CDR API expects JSON payload
    }
    
    try:
        response = requests.post(
            config.PBX_URL_CDR,
            json=request_payload_body_cdr, # Send as JSON
            headers=headers_cdr,
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        if data.get('has_error', False):
            error_msg = f"CDR API Error on page {page_number}: {data.get('messages', 'Unknown API error')}"
            return page_number, None, error_msg, data.get('pagination') # Still return pagination if available
        
        return page_number, data.get('results', []), None, data.get('pagination') # Success
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP Error fetching CDR page {page_number}: {e}. Response: {e.response.text[:200]}"
        return page_number, None, error_msg, None
    except Exception as e:
        error_msg = f"General Error fetching CDR page {page_number}: {e}"
        # traceback.print_exc() # Can be noisy, enable if needed
        return page_number, None, error_msg, None

# Example of how this module might be tested directly (optional)
if __name__ == '__main__':
    print("Testing api_client.py...")
    
    # Test user status fetch
    print("\n--- Testing User Status Fetch ---")
    user_data = fetch_user_status_data()
    if user_data:
        print(f"Fetched {len(user_data)} users. Sample:")
        count = 0
        for username, ext in user_data.items():
            print(f"  {username}: {ext}")
            count += 1
            if count >= 3: break
    else:
        print("Failed to fetch user data or no users found.")

    # Test CDR page fetch (e.g., for yesterday, first page)
    print("\n--- Testing CDR Page Fetch (Page 1 for yesterday) ---")
    from datetime import date, timedelta
    yesterday_dt = date.today() - timedelta(days=1)
    test_date = yesterday_dt.strftime('%Y-%m-%d')
    start_dt_str = f"{test_date} 00:00:00"
    end_dt_str = f"{test_date} 23:59:59"
    
    pg_num, results, error, pagination_info = fetch_cdr_api_page(1, start_dt_str, end_dt_str)
    if error:
        print(f"Error fetching CDR page 1: {error}")
    elif results is not None: # results can be an empty list
        print(f"Fetched page {pg_num} with {len(results)} calls.")
        if pagination_info:
            print(f"Pagination: {pagination_info}")
        if results:
            print("Sample call data (first call):")
            print(json.dumps(results[0], indent=2))
    else:
        print("CDR page fetch returned None for results without a specific error message (should not happen).")