#!/usr/bin/env python
"""
Timeline Data Helper - Preprocesses CSV data for the email timeline visualization.

This script reads the output from schedule_org_emails.py, computes exclusion windows, 
and generates a modified CSV file that includes all necessary timeline visualization data.

Usage:
    python timeline_data_helper.py --input-csv scheduled_emails.csv --output-csv timeline_data.csv [options]
    
    Options:
        --input-csv FILE          Input CSV file with scheduled emails (required)
        --output-csv FILE         Output CSV file for timeline visualization (required)
        --contact-id INT          Filter by specific contact ID (optional)
        --include-exclusions      Include calculated exclusion windows (default: True)
"""

import argparse
import csv
from datetime import date, datetime, timedelta
import json
import sys
import os


# Constants from email_scheduler_common.py
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

PRE_WINDOW_EXCLUSION_DAYS = 60

def parse_date(date_str):
    """Parse a date string in various formats to a datetime object"""
    if not date_str or date_str.strip() == '':
        return None
        
    # Try different formats
    formats = ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    print(f"Warning: Could not parse date '{date_str}'")
    return None

def calculate_rule_windows(contact):
    """Calculate rule windows based on contact's state and dates"""
    windows = []
    state = contact['state']
    
    # Parse birth date and effective date
    birth_date = parse_date(contact['birth_date'])
    effective_date = parse_date(contact['effective_date'])
    
    if not birth_date and not effective_date:
        return windows
    
    # Get current date and end date (2 years from now)
    current_date = date.today()
    end_date = current_date + timedelta(days=365 * 2)
    
    # Calculate birthdays in our date range
    birthdays = []
    if birth_date:
        for year in range(current_date.year, end_date.year + 1):
            try:
                birthday = date(year, birth_date.month, birth_date.day)
                if birthday >= current_date - timedelta(days=365) and birthday <= end_date:
                    birthdays.append(birthday)
            except ValueError:
                # Handle Feb 29 in non-leap years
                if birth_date.month == 2 and birth_date.day == 29 and not is_leap_year(year):
                    birthday = date(year, 2, 28)
                    if birthday >= current_date - timedelta(days=365) and birthday <= end_date:
                        birthdays.append(birthday)
    
    # Calculate effective dates in our date range
    effective_dates = []
    if effective_date:
        for year in range(current_date.year, end_date.year + 1):
            try:
                eff_date = date(year, effective_date.month, effective_date.day)
                if eff_date >= current_date - timedelta(days=365) and eff_date <= end_date:
                    effective_dates.append(eff_date)
            except ValueError:
                # Handle Feb 29 in non-leap years
                if effective_date.month == 2 and effective_date.day == 29 and not is_leap_year(year):
                    eff_date = date(year, 2, 28)
                    if eff_date >= current_date - timedelta(days=365) and eff_date <= end_date:
                        effective_dates.append(eff_date)
    
    # Process birthday rule states
    if state in BIRTHDAY_RULE_STATES:
        window_before = BIRTHDAY_RULE_STATES[state]["window_before"]
        window_after = BIRTHDAY_RULE_STATES[state]["window_after"]
        
        for bd in birthdays:
            # Special handling for Nevada (first day of birth month)
            if state == "NV":
                bd = date(bd.year, bd.month, 1)
            
            # Calculate rule window
            window_start = bd - timedelta(days=window_before)
            window_end = bd + timedelta(days=window_after)
            
            windows.append({
                "type": "birthday",
                "start_date": window_start.isoformat(),
                "end_date": window_end.isoformat(),
                "description": f"{state} birthday rule window ({window_before} days before, {window_after} days after)"
            })
    
    # Process effective date rule states
    if state in EFFECTIVE_DATE_RULE_STATES:
        window_before = EFFECTIVE_DATE_RULE_STATES[state]["window_before"]
        window_after = EFFECTIVE_DATE_RULE_STATES[state]["window_after"]
        
        for ed in effective_dates:
            # Calculate rule window
            window_start = ed - timedelta(days=window_before)
            window_end = ed + timedelta(days=window_after)
            
            windows.append({
                "type": "effective_date",
                "start_date": window_start.isoformat(),
                "end_date": window_end.isoformat(),
                "description": f"{state} effective date rule window ({window_before} days before, {window_after} days after)"
            })
    
    return windows

def calculate_exclusion_periods(rule_windows):
    """Calculate exclusion periods from rule windows"""
    exclusions = []
    
    for window in rule_windows:
        # Parse dates
        window_start = parse_date(window["start_date"])
        window_end = parse_date(window["end_date"])
        
        # Calculate extended exclusion (PRE_WINDOW_EXCLUSION_DAYS before window to window end)
        exclusion_start = window_start - timedelta(days=PRE_WINDOW_EXCLUSION_DAYS)
        exclusion_end = window_end
        
        exclusions.append({
            "type": "exclusion",
            "start_date": exclusion_start.isoformat(),
            "end_date": exclusion_end.isoformat(),
            "description": f"Exclusion period for {window['type']} rule window"
        })
    
    return exclusions

def is_leap_year(year):
    """Check if a year is a leap year"""
    if year % 400 == 0:
        return True
    if year % 100 == 0:
        return False
    return year % 4 == 0

def read_csv(file_path):
    """Read CSV file and return as list of dictionaries"""
    data = []
    
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            data.append(row)
    
    return data

def write_csv(data, file_path):
    """Write list of dictionaries to CSV file"""
    if not data:
        print("No data to write to CSV")
        return
    
    # Get fieldnames from first row
    fieldnames = list(data[0].keys())
    
    with open(file_path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Wrote {len(data)} rows to {file_path}")

def process_data(input_csv, output_csv, contact_id=None, include_exclusions=True):
    """Process data and write to output CSV"""
    # Read input CSV
    data = read_csv(input_csv)
    print(f"Read {len(data)} rows from {input_csv}")
    
    # Filter by contact_id if specified
    if contact_id:
        data = [row for row in data if row['contact_id'] == str(contact_id)]
        print(f"Filtered to {len(data)} rows for contact {contact_id}")
    
    # Group by contact
    contacts = {}
    for row in data:
        contact_id = row['contact_id']
        
        if contact_id not in contacts:
            contacts[contact_id] = {
                'org_id': row['org_id'],
                'contact_id': contact_id,
                'email': row['email'],
                'first_name': row['first_name'],
                'last_name': row['last_name'],
                'state': row['state'],
                'birth_date': row['birth_date'],
                'effective_date': row['effective_date'],
                'emails': []
            }
        
        # Add email to contact if it's an email type
        if 'email_type' in row and row['email_type']:
            email_type = row['email_type'].lower()
            if '(skipped)' in email_type:
                email_type = email_type.replace(' (skipped)', '')
                skipped = 'Yes'
            else:
                skipped = row.get('skipped', 'No')
                
            contacts[contact_id]['emails'].append({
                'type': email_type,
                'date': row.get('email_date', ''),
                'skipped': skipped,
                'reason': row.get('reason', ''),
                'link': row.get('link', '')
            })
    
    # Calculate rule windows and exclusion periods
    output_data = []
    
    for contact_id, contact in contacts.items():
        rule_windows = calculate_rule_windows(contact)
        exclusion_periods = []
        
        if include_exclusions:
            exclusion_periods = calculate_exclusion_periods(rule_windows)
        
        # Add emails to output data
        for email in contact['emails']:
            output_row = {
                'org_id': contact['org_id'],
                'contact_id': contact['contact_id'],
                'email': contact['email'],
                'first_name': contact['first_name'],
                'last_name': contact['last_name'],
                'state': contact['state'],
                'birth_date': contact['birth_date'],
                'effective_date': contact['effective_date'],
                'item_type': email['type'],
                'date': email['date'],
                'end_date': '',  # Emails are single-point events
                'skipped': email['skipped'],
                'reason': email['reason'],
                'link': email['link'],
                'description': f"{email['type'].title()} email" + (" (skipped)" if email['skipped'] == 'Yes' else "")
            }
            
            output_data.append(output_row)
        
        # Add rule windows to output data
        for window in rule_windows:
            output_row = {
                'org_id': contact['org_id'],
                'contact_id': contact['contact_id'],
                'email': contact['email'],
                'first_name': contact['first_name'],
                'last_name': contact['last_name'],
                'state': contact['state'],
                'birth_date': contact['birth_date'],
                'effective_date': contact['effective_date'],
                'item_type': 'rule_window',
                'date': window['start_date'],
                'end_date': window['end_date'],
                'skipped': 'No',
                'reason': '',
                'link': '',
                'description': window['description']
            }
            
            output_data.append(output_row)
        
        # Add exclusion periods to output data
        for exclusion in exclusion_periods:
            output_row = {
                'org_id': contact['org_id'],
                'contact_id': contact['contact_id'],
                'email': contact['email'],
                'first_name': contact['first_name'],
                'last_name': contact['last_name'],
                'state': contact['state'],
                'birth_date': contact['birth_date'],
                'effective_date': contact['effective_date'],
                'item_type': 'exclusion',
                'date': exclusion['start_date'],
                'end_date': exclusion['end_date'],
                'skipped': 'No',
                'reason': '',
                'link': '',
                'description': exclusion['description']
            }
            
            output_data.append(output_row)
    
    # Write output CSV
    write_csv(output_data, output_csv)

def main():
    parser = argparse.ArgumentParser(description="Process email schedule data for timeline visualization")
    parser.add_argument("--input-csv", required=True, help="Input CSV file with scheduled emails")
    parser.add_argument("--output-csv", required=True, help="Output CSV file for timeline visualization")
    parser.add_argument("--contact-id", type=int, help="Filter by specific contact ID")
    parser.add_argument("--include-exclusions", action="store_true", default=True, help="Include calculated exclusion windows")
    
    args = parser.parse_args()
    
    # Check that input file exists
    if not os.path.exists(args.input_csv):
        print(f"Error: Input file {args.input_csv} does not exist")
        sys.exit(1)
    
    # Process data
    process_data(args.input_csv, args.output_csv, args.contact_id, args.include_exclusions)

if __name__ == "__main__":
    main()