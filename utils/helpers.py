# nps_processor/utils/helpers.py

import re
from datetime import datetime

# Pre-compiled Regex Patterns
CHANNEL_USERNAME_REGEX = re.compile(r'(?:PJSIP|SIP)/([a-zA-Z0-9_-]+)', re.IGNORECASE)
LOCAL_CHANNEL_USERNAME_REGEX = re.compile(r'Local/([a-zA-Z0-9_.-]+)@', re.IGNORECASE)

def parse_call_time(time_str):
    """Parses the API time string into date and hour strings."""
    if not time_str:
        return "N/A", "N/A"
    try:
        dt_obj = datetime.fromisoformat(time_str.replace(" ", "T"))
        return dt_obj.strftime('%Y-%m-%d'), dt_obj.strftime('%H:%M:%S')
    except ValueError:
        return "PARSE_ERROR", "PARSE_ERROR"

def get_employee_first_name(fullname):
    """Extracts the first name from a full name string."""
    if not fullname or fullname == "N/A":
        return "N/A"
    return fullname.split(' ')[0]

def extract_username_from_channel(channel_str):
    """Extracts a potential username from channel strings using pre-compiled regex."""
    if not channel_str:
        return None
    match = CHANNEL_USERNAME_REGEX.search(channel_str)
    if match:
        return match.group(1)
    match_local = LOCAL_CHANNEL_USERNAME_REGEX.search(channel_str)
    if match_local:
        return match_local.group(1)
    return None

def clean_phone_for_db_lookup(raw_phone):
    """
    Cleans a phone number to a canonical format (e.g., 407xxxxxxx)
    for consistent database lookups.
    """
    if not raw_phone:
        return None
    
    cleaned = str(raw_phone).replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    
    if cleaned.startswith('+'):
        cleaned = cleaned[1:]
    
    if cleaned.startswith('00'): # e.g. 00407...
        cleaned = cleaned[2:]
    elif cleaned.startswith('0'): # e.g. 07...
        # Assuming RO local numbers if they start with 0 and are 10 digits
        if len(cleaned) == 10 and (cleaned[1] in '723'): # RO 07xx, 02xx, 03xx
            cleaned = '40' + cleaned[1:]
        # Else, it might be an international number that starts with 0 but isn't RO prefix,
        # or just a short number. We'll rely on length check later.
    elif len(cleaned) == 9 and (cleaned[0] in '723'): # e.g. 7xxxxxxxx for RO
        cleaned = '40' + cleaned
    # If already starts with a country code like 40, 44, etc., and is long enough, assume it's okay.
    
    return cleaned if len(cleaned) >= 9 else None # Basic length check for validity