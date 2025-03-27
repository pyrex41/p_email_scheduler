"""
Test script to verify the fix for calculate_post_window_dates function.
Focuses on handling leap year birthdays in Nevada with contact ID 702.
"""

import json
from datetime import date, datetime
import sys
from email_scheduler_common import (
    calculate_rule_windows, calculate_post_window_dates, is_leap_year, BIRTHDAY_RULE_STATES
)

def main():
    # Test case for contact 702 (Nevada leap year birthday)
    contact = {
        "id": 702,
        "state": "NV",
        "birth_date": "1960-02-29",
        "effective_date": "2000-03-01",
        "age": 64
    }
    
    # Set up test parameters
    current_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    
    # Extract birthday for rule window calculation
    original_birthday = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
    birthdays = []
    
    # Add current year birthday (handle Feb 29 in non-leap years)
    if original_birthday.month == 2 and original_birthday.day == 29 and not is_leap_year(current_date.year):
        birthdays.append(date(current_date.year, 2, 28))
    else:
        birthdays.append(date(current_date.year, original_birthday.month, original_birthday.day))
    
    # Log inputs
    print(f"Testing calculate_post_window_dates fix for contact {contact['id']} (NV leap year birthday)")
    print(f"Contact birth date: {original_birthday}")
    print(f"Current date: {current_date}")
    print(f"End date: {end_date}")
    print(f"Birthdays: {birthdays}")
    print("\n")
    
    # Calculate rule windows
    rule_windows = calculate_rule_windows(contact, birthdays, [], current_date, end_date)
    print(f"Rule windows: {rule_windows}")
    
    # Calculate post-window dates
    post_window_dates = calculate_post_window_dates(rule_windows, end_date)
    print(f"Post-window dates: {post_window_dates}")
    
    # Check if expected date (March 31) is found
    expected_date = date(2024, 3, 31)
    if expected_date in post_window_dates:
        print("\nSUCCESS: Found expected post-window date (March 31, 2024)")
        return 0
    else:
        print("\nFAILURE: Expected post-window date (March 31, 2024) not found")
        return 1

if __name__ == "__main__":
    sys.exit(main())