"""
Common functions and logic for both synchronous and asynchronous email schedulers.
Simplified version that only includes essential constants and utility functions.
"""

from datetime import date, datetime, timedelta
import logging
import os
import json
from typing import Optional

# Configure logging
LOG_FILE = os.environ.get('LOG_FILE', 'logs/email_scheduler.log')
logger = logging.getLogger("email_scheduler")
logger.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Create file handler
try:
    # Ensure the directory exists
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
        
    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
except Exception as e:
    print(f"Warning: Could not set up log file at {LOG_FILE}: {e}")
    print(f"Using fallback log file: email_scheduler.log")
    # Fallback to local log file
    fallback_handler = logging.FileHandler('email_scheduler.log', mode='a')
    fallback_handler.setFormatter(formatter)
    logger.addHandler(fallback_handler)

# All US states
ALL_STATES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC'
]

# Email type constants
EMAIL_TYPE_BIRTHDAY = "birthday"
EMAIL_TYPE_EFFECTIVE_DATE = "effective_date"
EMAIL_TYPE_AEP = "aep"
EMAIL_TYPE_POST_WINDOW = "post_window"

# Load ZIP code data
try:
    with open('zipData.json') as f:
        ZIP_DATA = json.load(f)
except Exception as e:
    logger.error(f"Failed to load zipData.json: {e}")
    ZIP_DATA = {}

def get_state_from_zip(zip_code: str) -> Optional[str]:
    """
    Get state from ZIP code using zipData.json
    
    Args:
        zip_code: ZIP code as string
        
    Returns:
        Two-letter state code, or None if not found
    """
    try:
        if not zip_code or not str(zip_code).strip():
            return None
        # Convert to string and take first 5 digits
        zip_str = str(zip_code)[:5]
        if zip_str in ZIP_DATA:
            return ZIP_DATA[zip_str]['state']
    except (KeyError, TypeError, ValueError) as e:
        logger.warning(f"Error getting state for ZIP {zip_code}: {e}")
    return None

# Helper function to check if a year is a leap year
def is_leap_year(year):
    """Returns True if the given year is a leap year, False otherwise"""
    if year % 400 == 0:
        return True
    if year % 100 == 0:
        return False
    return year % 4 == 0

# Helper function to safely create a date
def try_create_date(year, month, day):
    """
    Attempts to create a date, handling leap year dates consistently
    For February 29 in non-leap years, uses February 28 instead
    """
    try:
        return date(year, month, day)
    except ValueError:
        # Handle February 29 in non-leap years
        if month == 2 and day == 29:
            return date(year, 2, 28)  # Use February 28 in non-leap years
        return None

# Helper function to check if a date is the last day of the month
def is_month_end(date_obj):
    """Check if a date is the last day of its month, handling leap years"""
    # Get the first day of the next month
    if date_obj.month == 12:
        next_month = date(date_obj.year + 1, 1, 1)
    else:
        next_month = date(date_obj.year, date_obj.month + 1, 1)
    
    # If the date is the day before the first of next month, it's the last day
    return (next_month - timedelta(days=1)) == date_obj

