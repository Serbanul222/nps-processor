# nps_processor/config.py

# --- API Configuration ---
PBX_URL_CDR = "https://lensa.while1.biz/api/cdr"
PBX_URL_USERS = "https://lensa.while1.biz/api/users-status"
API_TOKEN = "GfR7tVSCn|4Ug75%FGRe21" # Replace with your actual token
API_KEY = "cKTdsmwaOcCsUjvgcR6mZ"    # Replace with your actual API key

# --- Database Configuration ---
DB_HOST = "192.168.101.43"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "einherjaricuochelari2021" # Replace with your actual DB password
DB_NAME = "magazia_de_date"

# --- Script Behavior Configuration ---
MAX_PAGE_FETCH_WORKERS = 5    # Workers for fetching API pages
MAX_CALL_PROCESS_WORKERS = 10 # Workers for processing individual calls (after email batch lookup)
REQUEST_TIMEOUT = 60          # Seconds for API requests
DB_EMAIL_LOOKUP_BATCH_SIZE = 100 # How many phone numbers per IN clause for email lookup
NPS_COOLDOWN_DAYS = 7         # Days before a contact can receive another NPS

# --- Filtering Criteria ---
VALID_CALL_CODES_FOR_NPS = ['incoming', 'inbound_ivr', 'outbound'] # For the 'code' field
MIN_BILL_DURATION_SECONDS = 30

# --- Output Configuration ---
CSV_OUTPUT_DIRECTORY = "output"
CSV_FILENAME_PREFIX = "eligible_calls_"
CSV_HEADERS = [
    'Interaction reference', 'Date', 'Hour', 'Employee reference', 
    'Employee first name', 'Direction', 'Customer pho', 'Destination', 
    'Caller ID', 'Customer referen', 'Queue name', 'Call duration', 
    'Bill duratio', 'Hold time', 'Customer Email'
]

# --- Testing Configuration ---
# Set to a date string like "YYYY-MM-DD" or a list of date strings to override default (yesterday)
# Set to None for production (processes yesterday)
TEST_DATES_OVERRIDE = ["2025-05-22"] # Example: Process only May 22, 2025 for testing
#TEST_DATES_OVERRIDE = None 

# Set to a number to limit pages fetched per date for testing (e.g., 1 for first ~100 records)
# Set to None for production (fetches all pages)
TEST_MAX_PAGES_PER_DATE = 200
#TEST_MAX_PAGES_PER_DATE = None