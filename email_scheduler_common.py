"""
Common functions and logic for both synchronous and asynchronous email schedulers.
This file contains shared code that ensures consistent behavior across both implementations.
"""

from datetime import date, datetime, timedelta
import logging
from typing import Dict, List, Set, Tuple, Optional, Union, Any

# Configure logging
import os

# Get log file path from environment variable with default
LOG_FILE = os.environ.get('LOG_FILE', 'logs/email_scheduler.log')

# Check if console output is enabled (default: False in production, True in development)
CONSOLE_OUTPUT = os.environ.get('CONSOLE_OUTPUT', '').lower() in ('true', '1', 'yes', 'y', 't')

# Create logger
logger = logging.getLogger("email_scheduler")
logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicates when module is reloaded
if logger.hasHandlers():
    logger.handlers.clear()

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

# Add console handler if enabled
if CONSOLE_OUTPUT:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Email type constants
EMAIL_TYPE_BIRTHDAY = "birthday"
EMAIL_TYPE_EFFECTIVE_DATE = "effective_date"
EMAIL_TYPE_AEP = "aep"
EMAIL_TYPE_POST_WINDOW = "post_window"

# Configurable exclusion window (days before rule window)
PRE_WINDOW_EXCLUSION_DAYS = 60

# Define special rule states and window durations
BIRTHDAY_RULE_STATES = {
    "CA": {"window_before": 30, "window_after": 30},  # 60-day period starting 30 days before birthday
    "ID": {"window_before": 0, "window_after": 63},   # 63-day period starting on birthday
    "IL": {"window_before": 0, "window_after": 45},   # 45-day period starting on birthday
    "KY": {"window_before": 0, "window_after": 60},   # 60-day period following birthday
    "LA": {"window_before": 30, "window_after": 63},  # 93-day period starting 30 days before birthday
    "MD": {"window_before": 0, "window_after": 31},   # 31-day period starting on birthday
    "NV": {"window_before": 0, "window_after": 60},   # 60-day period starting first day of birth month
    "OK": {"window_before": 0, "window_after": 60},   # 60-day period starting on birthday
    "OR": {"window_before": 0, "window_after": 31}    # 31-day period starting on birthday
}

EFFECTIVE_DATE_RULE_STATES = {
    "MO": {"window_before": 30, "window_after": 33}   # 63-day period starting 30 days before anniversary
}

# Year-round enrollment states (no scheduled emails)
YEAR_ROUND_ENROLLMENT_STATES = {"CT", "MA", "NY", "WA"}

# Pre-calculated AEP weeks for each year
AEP_WEEKS = {
    2023: ['2023-08-18', '2023-08-25', '2023-09-01', '2023-09-07'],
    2024: ['2024-08-18', '2024-08-25', '2024-09-01', '2024-09-07'],
    2025: ['2025-08-18', '2025-08-25', '2025-09-01', '2025-09-07'],
    2026: ['2026-08-18', '2026-08-25', '2026-09-01', '2026-09-07'],
    2027: ['2027-08-18', '2027-08-25', '2027-09-01', '2027-09-07'],
}

# Logging utility function
def log(message, always=False, debug=False):
    """
    Utility function for conditional logging
    - always: Always log at INFO level
    - debug: Log at DEBUG level (only shown when DEBUG=True)
    - Otherwise: Log at INFO level when VERBOSE=True
    """
    if debug:
        logger.debug(message)
    elif always:
        logger.info(message)
    else:
        logger.debug(message)  # Use debug by default for verbose logs

# Check if a year is a leap year
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

# Helper function to get all occurrences of a date (birthdays, etc.) in a date range
def get_all_occurrences(event_day, start, end_date):
    """Get all occurrences of a date (e.g. birthdays) in the given range"""
    dates = []
    # Also check the year before start if it might result in valid email dates
    for yr in range(start.year - 1, end_date.year + 1):
        date_obj = try_create_date(yr, event_day.month, event_day.day)
        if date_obj:  # Don't filter by date range here, let the caller handle that
            dates.append(date_obj)
    return sorted(dates)  # Return dates in chronological order

# Function to calculate rule windows based on state-specific rules
def calculate_rule_windows(contact, birthdays, effective_dates, current_date, end_date):
    """
    Calculate rule windows for a contact based on their state and dates
    
    Args:
        contact: Contact dictionary with state and dates
        birthdays: List of birthday dates (can be empty if calculating from contact)
        effective_dates: List of effective dates (can be empty if calculating from contact)
        current_date: Current date to start calculations from
        end_date: End date to stop calculations at
        
    Returns:
        List of tuples (window_start, window_end, rule_type, state)
    """
    rule_windows = []
    state = contact.get('state', 'CA')  # Default to CA if no state
    
    # Skip for year-round enrollment states
    if state in YEAR_ROUND_ENROLLMENT_STATES:
        logger.debug(f"Contact {contact['id']} is in year-round enrollment state {state}, no rule windows apply")
        return []
    
    # If no birthdays provided, calculate from contact birth_date
    if not birthdays and contact.get('birth_date'):
        try:
            # Handle both date objects and strings
            if isinstance(contact['birth_date'], date):
                original_birthday = contact['birth_date']
            elif isinstance(contact['birth_date'], str):
                # Try multiple date formats
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m-%d-%Y"]:
                    try:
                        original_birthday = datetime.strptime(contact['birth_date'], fmt).date()
                        break
                    except ValueError:
                        continue
                else:  # No format worked
                    original_birthday = None
            else:
                original_birthday = None
                
            if original_birthday:
                # Calculate birthdays in range
                if (original_birthday.month > current_date.month or 
                    (original_birthday.month == current_date.month and 
                     original_birthday.day >= current_date.day)):
                    birthdays.append(date(current_date.year, original_birthday.month, original_birthday.day))
                
                for yr in range(current_date.year + 1, end_date.year + 1):
                    if original_birthday.month == 2 and original_birthday.day == 29 and not is_leap_year(yr):
                        birthdays.append(date(yr, 2, 28))
                    else:
                        birthdays.append(date(yr, original_birthday.month, original_birthday.day))
        except Exception as e:
            logger.warning(f"Error processing birth date for contact: {e}")
    
    # If no effective dates provided, calculate from contact effective_date
    if not effective_dates and contact.get('effective_date'):
        try:
            # Handle both date objects and strings
            if isinstance(contact['effective_date'], date):
                original_effective_date = contact['effective_date']
            elif isinstance(contact['effective_date'], str):
                # Try multiple date formats
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m-%d-%Y"]:
                    try:
                        original_effective_date = datetime.strptime(contact['effective_date'], fmt).date()
                        break
                    except ValueError:
                        continue
                else:  # No format worked
                    original_effective_date = None
            else:
                original_effective_date = None
                
            if original_effective_date:
                # Calculate effective dates in range
                for yr in range(current_date.year, end_date.year + 1):
                    effective_dates.append(date(yr, original_effective_date.month, original_effective_date.day))
        except Exception as e:
            logger.warning(f"Error processing effective date for contact: {e}")
    
    # Process birthday rule states
    if state in BIRTHDAY_RULE_STATES and birthdays:
        window_before = BIRTHDAY_RULE_STATES[state]["window_before"]
        window_after = BIRTHDAY_RULE_STATES[state]["window_after"]
        
        for birthday in birthdays:
            # Special handling for Nevada (first day of birth month)
            if state == "NV":
                birthday = date(birthday.year, birthday.month, 1)
            
            # Calculate rule window
            rule_window_start = birthday - timedelta(days=window_before)
            rule_window_end = birthday + timedelta(days=window_after)
            
            # Only consider windows that overlap with our target date range
            if rule_window_start <= end_date and rule_window_end >= current_date:
                logger.debug(f"Birthday rule window for {state} contact: {rule_window_start} to {rule_window_end}")
                rule_windows.append((rule_window_start, rule_window_end, "birthday", state))
    
    # Process effective date rule states
    if state in EFFECTIVE_DATE_RULE_STATES and effective_dates:
        window_before = EFFECTIVE_DATE_RULE_STATES[state]["window_before"]
        window_after = EFFECTIVE_DATE_RULE_STATES[state]["window_after"]
        
        for eff_date in effective_dates:
            # Calculate rule window
            rule_window_start = eff_date - timedelta(days=window_before)
            rule_window_end = eff_date + timedelta(days=window_after)
            
            # Only consider windows that overlap with our target date range
            if rule_window_start <= end_date and rule_window_end >= current_date:
                logger.debug(f"Effective date rule window for {state} contact: {rule_window_start} to {rule_window_end}")
                rule_windows.append((rule_window_start, rule_window_end, "effective_date", state))
    
    # For states without specific rules, return an empty list (no rule windows)
    # This is a key change - no default 60-day window for states without specific rules
    if not rule_windows and state not in YEAR_ROUND_ENROLLMENT_STATES:
        logger.debug(f"Contact {contact['id']} is in state {state} with no specific rule windows defined")
        return []  # Return empty list for states like Kansas with no specific rules
    
    return rule_windows

# Class to represent a date range for exclusion periods
class DateRange:
    def __init__(self, start_date, end_date):
        self.start = start_date
        self.end_date = end_date

# Helper function to safely create a DateRange
def create_daterange(start, end_date):
    if start > end_date:
        return None
    return DateRange(start, end_date)

# Function to calculate exclusion periods
def calculate_exclusion_periods(rule_windows, current_date, end_date):
    """
    Calculate exclusion periods from rule windows
    Returns: List of DateRange objects representing exclusion periods
    """
    exclusions = []
    
    for rule_window_start, rule_window_end, rule_type, state in rule_windows:
        # Calculate extended exclusion (PRE_WINDOW_EXCLUSION_DAYS before window to window end)
        exclusion_start = rule_window_start - timedelta(days=PRE_WINDOW_EXCLUSION_DAYS)
        exclusion_end = rule_window_end
        
        # Log for debugging
        logger.debug(f"For {rule_type} rule window [{rule_window_start} to {rule_window_end}], exclusion period: {exclusion_start} to {exclusion_end}")
        
        # Bound the exclusion by the current date and end date
        bounded_start = max(exclusion_start, current_date - timedelta(days=PRE_WINDOW_EXCLUSION_DAYS))
        bounded_end = min(exclusion_end, end_date)
        
        # Create a DateRange object if we have a valid range
        if bounded_start <= bounded_end:
            exclusion = DateRange(bounded_start, bounded_end)
            exclusions.append(exclusion)
    
    # Sort exclusions by start date
    exclusions.sort(key=lambda x: x.start)
    return exclusions

# Function to calculate post-window dates
def calculate_post_window_dates(rule_windows, end_date):
    """
    Calculate post-window dates based on rule windows
    Returns: List of dates representing post-window dates
    """
    post_window_dates = []
    
    # Log the number of rule windows we're processing
    logger.debug(f"Processing {len(rule_windows)} rule window(s) for post-window dates")
    
    if not rule_windows:
        logger.debug("No rule windows found, cannot calculate post-window dates")
        return post_window_dates
    
    for rule_window_start, rule_window_end, rule_type, state in rule_windows:
        # Skip if this is not a birthday rule
        if rule_type != "birthday":
            logger.debug(f"Skipping {rule_type} rule window for post-window calculation")
            continue
            
        # Default: post-window date is the day after the rule window ends
        post_window_date = rule_window_end + timedelta(days=1)
        
        # Log the rule window we're processing
        logger.debug(f"Calculating post-window date for {state} {rule_type} rule window: {rule_window_start} to {rule_window_end}")
        
        # General rule for NV state (first-of-month rule state)
        # If the rule window starts on the first day of a month AND
        # the rule window end date is the last day of its month,
        # use the end date as the post-window date
        if state == "NV" and rule_window_start.day == 1 and is_month_end(rule_window_end):
            post_window_date = rule_window_end
            logger.debug(f"Nevada-style rule detected: Using end date {post_window_date} instead")
        
        # Logic for February birthdays across all states
        if rule_window_start.month == 2:
            # Special handling for February 29 birthdays (leap year birthdays)
            if rule_window_start.day == 29:
                # For CA contacts with Feb 29 birthday: post-window on March 30
                if state == 'CA':
                    post_window_date = date(rule_window_end.year, 3, 30)
                    logger.debug(f"Special case: Feb 29 CA birthday, post-window date: {post_window_date}")
                
                # For NV contacts with Feb 29 birthday: post-window on March 31
                elif state == 'NV':
                    # Always use March 31 for NV contacts with Feb 29 birthday
                    post_window_date = date(rule_window_end.year, 3, 31)
                    logger.debug(f"Special case: Feb 29 NV birthday, post-window date: {post_window_date}")
            # Handle other February birthdays
            elif rule_window_end.month == 3 and (rule_window_end.day == 29 or rule_window_end.day == 30):
                if state == 'CA' and rule_window_start.day < 15 and rule_window_start.day != 1:  # Before mid-month, not 1st
                    # Set to end of March
                    if rule_window_end.day == 29:
                        post_window_date = date(rule_window_end.year, 3, 30)
                        logger.debug(f"Special case: Early Feb CA birthday, post-window date: {post_window_date}")
                    elif rule_window_end.day == 30:
                        post_window_date = date(rule_window_end.year, 3, 31)
                        logger.debug(f"Special case: Early Feb CA birthday, post-window date: {post_window_date}")
        
        # Make sure the post window date falls within the next year
        # This is to handle cases where the rule window crosses year boundary
        if post_window_date.year < rule_window_end.year:
            post_window_date = date(rule_window_end.year, post_window_date.month, post_window_date.day)
            logger.debug(f"Adjusted post-window date to be in same year as rule end: {post_window_date}")
        
        # Only include dates that fall before our end date
        if post_window_date <= end_date:
            logger.debug(f"For {rule_type} rule window [{rule_window_start} to {rule_window_end}], post-window date: {post_window_date}")
            post_window_dates.append(post_window_date)
        else:
            logger.debug(f"Post-window date {post_window_date} is beyond our end date {end_date}, skipping")
    
    # Sort post-window dates chronologically
    post_window_dates.sort()
    logger.debug(f"Calculated {len(post_window_dates)} post-window date(s): {', '.join(str(d) for d in post_window_dates)}")
    
    return post_window_dates

# Calculate birthday email date, including special handling for February 29
def calculate_birthday_email_date(birthday_date, email_year):
    """
    Calculate the date to send a birthday email based on the recipient's birthday
    
    Args:
        birthday_date: The contact's birthday
        email_year: The year in which to send the email
        
    Returns:
        The date on which to send the birthday email
    """
    # Extract month and day from birthday
    month = birthday_date.month
    day = birthday_date.day
    
    # Special handling for February 29 birthdays
    if month == 2 and day == 29:
        # For February 29 birthdays, always send email on February 14
        # regardless of whether it's a leap year or not
        return date(email_year, 2, 14)
    
    # For all other birthdays, create the birthday in the email year
    email_year_birthday = try_create_date(email_year, month, day)
    
    # Send the email 14 days before the birthday
    return email_year_birthday - timedelta(days=14)

# Calculate the effective date email date 
def calculate_effective_date_email(effective_date, current_date):
    """
    Calculate when to send an effective date email, handling special cases
    
    Args:
        effective_date: The policy effective date
        current_date: The current date (to handle boundary cases)
        
    Returns:
        The date on which to send the effective date email
    """
    # Standard rule: Send email 30 days before effective date
    email_date = effective_date - timedelta(days=30)
    
    # Special handling for January effective dates where email
    # falls in previous year December
    if effective_date.month == 1 and effective_date.day <= 30:
        # For effective dates in early January, emails would
        # land in December of previous year
        prev_year = effective_date.year - 1
        
        # For January 1 effective dates, send email on December 2 of previous year
        if effective_date.day == 1:
            return date(prev_year, 12, 2)
        
        # For other early January dates, calculate based on the 30-day rule
        # Note: February 15 effective date would result in January 16 email date (30 days prior)
        return email_date
    
    return email_date

# Function to get precomputed AEP dates
def precompute_aep_dates(current_date, end_date):
    """Return precomputed AEP dates for a range of years"""
    aep_dates_by_year = {}
    
    for year in range(current_date.year, end_date.year + 1):
        aep_dates_by_year[year] = get_aep_dates_for_year(year)
    
    return aep_dates_by_year

def get_aep_dates_for_year(year):
    """Get the standard AEP email dates for a given year"""
    return [
        date(year, 8, 18),  # Week 1
        date(year, 8, 25),  # Week 2
        date(year, 9, 1),   # Week 3
        date(year, 9, 7)    # Week 4
    ]

# For backwards compatibility with existing code
def get_aep_dates(year):
    """Get the standard AEP email dates for a given year (alias for get_aep_dates_for_year)"""
    return get_aep_dates_for_year(year)

def find_valid_aep_date(contact, exclusion_periods, aep_dates, current_date, end_date):
    """Find a valid AEP date that doesn't fall in any exclusion period"""
    
    original_birthday = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
    
    # Start with assigned week based on standard distribution
    contact_index = int(contact['id']) % len(aep_dates)
    assigned_date = aep_dates[contact_index]
    
    # For October birthdays, prioritize earlier dates to avoid exclusion windows
    if original_birthday.month == 10:
        # Try each AEP date in order (earliest first)
        for aep_date in sorted(aep_dates):
            # Skip dates outside our date range
            if not (current_date <= aep_date <= end_date):
                continue
                
            # Check if date is excluded
            excluded = False
            for exclusion in exclusion_periods:
                if exclusion.start <= aep_date <= exclusion.end_date:
                    excluded = True
                    break
                    
            # If not excluded, use this date
            if not excluded:
                return aep_date
    else:
        # For non-October birthdays, try assigned date first, then others
        dates_to_try = [assigned_date] + [d for d in aep_dates if d != assigned_date]
        
        for aep_date in dates_to_try:
            # Skip dates outside our date range
            if not (current_date <= aep_date <= end_date):
                continue
                
            # Check if date is excluded
            excluded = False
            for exclusion in exclusion_periods:
                if exclusion.start <= aep_date <= exclusion.end_date:
                    excluded = True
                    break
                    
            # If not excluded, use this date
            if not excluded:
                return aep_date
    
    # If we couldn't find a valid date, default to the assigned date
    # (even if it falls in an exclusion period)
    if current_date <= assigned_date <= end_date:
        return assigned_date
        
    # Last resort: return the earliest AEP date in our range
    for aep_date in sorted(aep_dates):
        if current_date <= aep_date <= end_date:
            return aep_date
            
    # If no valid dates at all, return None
    return None

from contact_rule_engine import ContactRuleEngine

# Initialize rule engine at module level
rule_engine = ContactRuleEngine()

def handle_special_post_window_cases(contact, current_date, end_date):
    """
    Returns a list of special post-window dates if applicable, or an empty list.
    
    Args:
        contact: The contact dictionary
        current_date: Current scheduling date
        end_date: End date for scheduling window
        
    Returns:
        List of post-window dates for special cases
    """
    try:
        return rule_engine.get_post_window_dates(contact, current_date)
    except Exception as e:
        logger.error(f"Error in handle_special_post_window_cases for contact {contact['id']}: {e}")
        return []

def handle_october_birthday_aep(contact, current_date):
    """
    Handle special AEP scheduling for October birthdays.
    Uses rule engine to determine appropriate AEP date.
    
    Args:
        contact: The contact dictionary
        current_date: Current scheduling date
        
    Returns:
        An AEP date if applicable, or None
    """
    try:
        return rule_engine.get_aep_date_override(contact, current_date)
    except Exception as e:
        logger.error(f"Error in handle_october_birthday_aep for contact {contact['id']}: {e}")
        return None

# Function to check if an email date is in an exclusion period
def is_date_excluded(date_obj, exclusions):
    """
    Check if a given date is within any exclusion period.
    
    Args:
        date_obj: The date to check
        exclusions: List of exclusion periods (DateRange objects)
        
    Returns:
        Boolean indicating if the date is excluded
    """
    # If exclusions list is empty (which will happen for states without rule windows like Kansas),
    # always return False - no exclusions apply
    if not exclusions:
        return False
        
    for exclusion in exclusions:
        if exclusion.start <= date_obj <= exclusion.end_date:
            return True
    return False

def should_force_aep_email(contact):
    """
    Determines if a contact should have an AEP email regardless of exclusion rules.
    
    Args:
        contact: The contact dictionary
        
    Returns:
        Boolean indicating if AEP email should be forced
    """
    return rule_engine.should_force_aep_email(contact)

# Function to schedule an email and track it in the context
def add_email_to_context(ctx, email_type, email_date, reason=None):
    """
    Add an email to the scheduling context.
    
    Args:
        ctx: The scheduling context
        email_type: Type of email (birthday, effective_date, aep, post_window)
        email_date: Date for the email
        reason: Optional reason for the email
        
    Returns:
        The email object that was added
    """
    # Create the email object
    email = {"type": email_type, "date": email_date.isoformat()}
    if reason:
        email["reason"] = reason
        
    # Add to scheduled emails list
    ctx.emails.append(email)
    
    # Add to scheduled dates list for exclusion checking
    ctx.scheduled_dates.append(email_date)
    
    return email