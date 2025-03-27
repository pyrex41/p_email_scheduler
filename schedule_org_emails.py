#!/usr/bin/env python
"""
Schedule Organization Emails - Specialized interface for the optimized email scheduler.

This script provides a specialized interface for scheduling emails for a specific organization,
pulling data from SQLite databases and outputting to CSV.

Usage:
    uv run schedule_org_emails.py --org-id <org_id> --output-csv <output_csv> [options]
    
    Options:
        --org-id INT             Organization ID (required)
        --output-csv FILE        Output CSV file path (required)
        --main-db FILE           Path to the main SQLite database (default: main.db)
        --org-db-dir DIR         Directory containing organization-specific databases (default: org_dbs/)
        --start-date YYYY-MM-DD  Start date for scheduling (default: today)
        --async                  Use asynchronous processing (faster for large datasets)
        --debug                  Enable debug logging
        --verbose                Enable verbose logging
"""

import argparse
import asyncio
import csv
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from email_scheduler_common import (
    logger, log, is_leap_year, try_create_date, EMAIL_TYPE_BIRTHDAY, 
    EMAIL_TYPE_EFFECTIVE_DATE, EMAIL_TYPE_AEP, EMAIL_TYPE_POST_WINDOW,
    BIRTHDAY_RULE_STATES, EFFECTIVE_DATE_RULE_STATES, YEAR_ROUND_ENROLLMENT_STATES,
    add_email_to_context, calculate_rule_windows, calculate_exclusion_periods
)
from email_scheduler_optimized import EmailScheduler, AsyncEmailProcessor
from contact_rule_engine import ContactRuleEngine

# Ensure correct constants
EMAIL_TYPE_BIRTHDAY = "birthday"
EMAIL_TYPE_EFFECTIVE_DATE = "effective_date"
EMAIL_TYPE_AEP = "aep"
EMAIL_TYPE_POST_WINDOW = "post_window"

# Define missing STATE_RULES
STATE_RULES = {
    state: {"type": "birthday", "window_before": info["window_before"], "window_after": info["window_after"]} 
    for state, info in BIRTHDAY_RULE_STATES.items()
}

# Add effective date states
for state, info in EFFECTIVE_DATE_RULE_STATES.items():
    STATE_RULES[state] = {"type": "effective_date", "window_before": info["window_before"], "window_after": info["window_after"]}

# Add year-round enrollment states
for state in YEAR_ROUND_ENROLLMENT_STATES:
    STATE_RULES[state] = {"type": "year_round"}

# Define missing EMAIL_RULES
EMAIL_RULES = {
    EMAIL_TYPE_BIRTHDAY: {
        "bypass_exclusion": False,
        "days_before": 14
    },
    EMAIL_TYPE_EFFECTIVE_DATE: {
        "bypass_exclusion": False,
        "days_before": 30
    },
    EMAIL_TYPE_AEP: {
        "bypass_exclusion": False,
        "distribution": lambda cid, n: int(cid) % n
    },
    EMAIL_TYPE_POST_WINDOW: {
        "bypass_exclusion": True
    }
}

# Define calculate_post_window_dates
def calculate_post_window_dates(rule_windows, end_date):
    """
    Calculate post-window dates from rule windows based on business rules
    
    Args:
        rule_windows: List of tuples (start_date, end_date, rule_type, state, birthday)
        end_date: End date for scheduling window
        
    Returns:
        List of post-window dates
    """
    post_window_dates = []
    for start, end, rule_type, state, birthday in rule_windows:
        # Add post window date 1 day after the window end
        post_date = end + timedelta(days=1)
        if post_date <= end_date:
            post_window_dates.append(post_date)
            
        # For California, add 31 days after rule window per state regulations
        if state == "CA":
            alt_post_date = end + timedelta(days=31)
            if alt_post_date <= end_date:
                post_window_dates.append(alt_post_date)
                
        # For states with birthday rules, add post-window based on window size
        if rule_type == "birthday" and state in BIRTHDAY_RULE_STATES:
            window_after = BIRTHDAY_RULE_STATES[state]["window_after"]
            birthday_post_date = end + timedelta(days=window_after)
            if birthday_post_date <= end_date:
                post_window_dates.append(birthday_post_date)
            
    return sorted(list(set(post_window_dates)))  # Remove duplicates and sort

# Helper function to check if a date is within an exclusion period
def is_date_excluded(date_obj, exclusions):
    """Check if a date is in an exclusion period"""
    for exclusion in exclusions:
        if exclusion.start <= date_obj <= exclusion.end_date:
            return True
    return False

# Create a simplified EmailScheduler implementation that works with our setup
class SimpleEmailScheduler(EmailScheduler):
    def __init__(self, current_date=None, end_date=None):
        self.current_date = current_date or date.today()
        self.end_date = end_date or (self.current_date + timedelta(days=365))
        self.rule_engine = ContactRuleEngine()
        self.aep_dates_by_year = self._precompute_aep_dates()
        
        # Validator for scheduled emails
        self.validator = type('MockValidator', (), {
            'validate_scheduled_emails': lambda *args: True,
            'validate_exclusions': lambda *args: True
        })()
    
    def process_contact(self, contact, contact_index=0):
        """Process a single contact to schedule all applicable emails"""
        result = {
            "contact_id": str(contact['id']),
            "emails": [],
            "skipped": []
        }
        
        try:
            # Init context for tracking emails and dates
            from email_scheduler_optimized import SchedulingContext
            ctx = SchedulingContext(self.current_date, self.end_date)
            
            # Skip blank contacts
            if not contact.get('birth_date') and not contact.get('effective_date'):
                result["skipped"].append({
                    "type": "all", 
                    "reason": "Missing both birth_date and effective_date"
                })
                return result
            
            # Process birthdays
            birthdays = []
            if contact.get('birth_date'):
                birthday = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
                
                # Current year birthday if not already passed
                if (birthday.month > self.current_date.month or 
                    (birthday.month == self.current_date.month and birthday.day >= self.current_date.day)):
                    birthdays.append(date(self.current_date.year, birthday.month, birthday.day))
                
                # Add birthdays for next year
                for yr in range(self.current_date.year + 1, self.end_date.year + 1):
                    # Handle Feb 29 in non-leap years
                    if birthday.month == 2 and birthday.day == 29 and not is_leap_year(yr):
                        birthdays.append(date(yr, 2, 28))
                    else:
                        birthdays.append(date(yr, birthday.month, birthday.day))
            
            # Process effective dates
            effective_dates = []
            if contact.get('effective_date'):
                effective_date = datetime.strptime(contact['effective_date'], "%Y-%m-%d").date()
                for yr in range(self.current_date.year, self.end_date.year + 1):
                    effective_dates.append(date(yr, effective_date.month, effective_date.day))
            
            # Get state and check if it's year-round
            state = contact.get('state', 'CA')
            if state in YEAR_ROUND_ENROLLMENT_STATES:
                # Skip all emails for year-round enrollment states
                for email_type in [EMAIL_TYPE_BIRTHDAY, EMAIL_TYPE_EFFECTIVE_DATE, EMAIL_TYPE_AEP, EMAIL_TYPE_POST_WINDOW]:
                    result["skipped"].append({
                        "type": email_type,
                        "reason": "Year-round enrollment state"
                    })
                return result
            
            # Calculate rule windows and exclusion periods
            rule_windows = calculate_rule_windows(contact, birthdays, effective_dates, self.current_date, self.end_date)
            exclusions = calculate_exclusion_periods(rule_windows, self.current_date, self.end_date)
            
            # Schedule birthday emails
            if birthdays:
                for birthday in sorted(birthdays):
                    # Birthday emails are 14 days before the birthday
                    email_date = birthday - timedelta(days=14)
                    if email_date >= self.current_date and email_date <= self.end_date:
                        # Check for IL age exception before scheduling birthday email
                        if state == "IL" and contact.get('birth_date'):
                            # Calculate age at the time of the birthday
                            birth_date = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
                            age_at_birthday = birthday.year - birth_date.year
                            # Skip birthday emails for IL residents 76+ years old
                            if age_at_birthday >= 76:
                                result["skipped"].append({
                                    "type": EMAIL_TYPE_BIRTHDAY,
                                    "reason": "Illinois resident over 76 years old"
                                })
                                break
                                
                        if not is_date_excluded(email_date, exclusions):
                            result["emails"].append({
                                "type": EMAIL_TYPE_BIRTHDAY,
                                "date": str(email_date)
                            })
                        else:
                            result["skipped"].append({
                                "type": EMAIL_TYPE_BIRTHDAY,
                                "reason": "Within exclusion period"
                            })
                        break  # Only schedule one birthday email
            
            # Schedule effective date emails
            if effective_dates:
                for eff_date in sorted(effective_dates):
                    # Effective date emails are 30 days before
                    email_date = eff_date - timedelta(days=30)
                    if email_date >= self.current_date and email_date <= self.end_date:
                        if not is_date_excluded(email_date, exclusions):
                            result["emails"].append({
                                "type": EMAIL_TYPE_EFFECTIVE_DATE,
                                "date": str(email_date)
                            })
                        else:
                            result["skipped"].append({
                                "type": EMAIL_TYPE_EFFECTIVE_DATE,
                                "reason": "Within exclusion period"
                            })
                        break  # Only schedule one effective date email
            
            # Schedule AEP emails
            aep_scheduled = False
            for yr in range(self.current_date.year, self.end_date.year + 1):
                # Use August/September AEP dates from the config
                aep_dates_for_year = self.rule_engine.get_aep_dates(yr)
                if not aep_dates_for_year:
                    continue
                
                # Distribute contacts evenly across the AEP weeks
                week_index = int(contact['id']) % len(aep_dates_for_year)
                aep_date = aep_dates_for_year[week_index]
                
                if aep_date >= self.current_date and aep_date <= self.end_date:
                    if not is_date_excluded(aep_date, exclusions):
                        result["emails"].append({
                            "type": EMAIL_TYPE_AEP,
                            "date": str(aep_date)
                        })
                        aep_scheduled = True
                        break  # Only schedule one AEP email per contact
            
            if not aep_scheduled:
                result["skipped"].append({
                    "type": EMAIL_TYPE_AEP,
                    "reason": "No suitable AEP date found"
                })
            
            # Calculate post-window dates
            post_window_dates = calculate_post_window_dates(rule_windows, self.end_date)
            
            # Schedule post-window emails
            post_window_scheduled = False
            if post_window_dates:
                for post_date in sorted(post_window_dates):
                    if post_date >= self.current_date and post_date <= self.end_date:
                        # Post-window emails bypass exclusion checks
                        result["emails"].append({
                            "type": EMAIL_TYPE_POST_WINDOW,
                            "date": str(post_date)
                        })
                        post_window_scheduled = True
                        break  # Only schedule one post-window email
            
            # Force post-window emails for each state in BIRTHDAY_RULE_STATES
            if not post_window_scheduled and state in BIRTHDAY_RULE_STATES and birthday:
                # For IL residents, check age before applying birthday window rules
                if state == "IL" and contact.get('birth_date'):
                    # Calculate age
                    birth_date = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
                    age = self.current_date.year - birth_date.year
                    if self.current_date.month < birth_date.month or (self.current_date.month == birth_date.month and self.current_date.day < birth_date.day):
                        age -= 1
                        
                    # Skip post-window email for IL residents 76+ years old
                    if age >= 76:
                        result["skipped"].append({
                            "type": EMAIL_TYPE_POST_WINDOW,
                            "reason": "Illinois resident over 76 years old"
                        })
                        return result
                
                # Schedule post-window email 1 day after the biggest birthday rule window
                window_after = BIRTHDAY_RULE_STATES[state]["window_after"]
                
                # Find the earliest birthday in our date range
                if birthdays:
                    earliest_birthday = sorted(birthdays)[0]
                    post_date = earliest_birthday + timedelta(days=window_after + 1)
                    
                    if post_date >= self.current_date and post_date <= self.end_date:
                        result["emails"].append({
                            "type": EMAIL_TYPE_POST_WINDOW,
                            "date": str(post_date),
                            "reason": "Forced post-window email"
                        })
                    else:
                        result["skipped"].append({
                            "type": EMAIL_TYPE_POST_WINDOW,
                            "reason": "Post-window date outside scheduling period"
                        })
                else:
                    result["skipped"].append({
                        "type": EMAIL_TYPE_POST_WINDOW,
                        "reason": "No birthdays in date range"
                    })
            elif not post_window_scheduled and state in BIRTHDAY_RULE_STATES:
                result["skipped"].append({
                    "type": EMAIL_TYPE_POST_WINDOW,
                    "reason": "No valid post-window dates found"
                })
            
            return result
            
        except Exception as e:
            log(f"Error processing contact {contact['id']}: {e}", always=True)
            result["skipped"].append({
                "type": "all",
                "reason": str(e)
            })
            return result

# Create simplified async processor
class SimpleAsyncEmailProcessor:
    """
    Simple asynchronous processor for email scheduling
    """
    
    def __init__(self, current_date=None, end_date=None, batch_size=100, max_workers=20):
        """Initialize the async processor with performance settings"""
        self.scheduler = SimpleEmailScheduler(current_date, end_date)
        self.batch_size = batch_size
        self.max_workers = max_workers
    
    async def process_contact_async(self, contact, index):
        """Process a single contact asynchronously"""
        return self.scheduler.process_contact(contact, index)
    
    async def process_batch(self, contacts_batch, start_index):
        """Process a batch of contacts concurrently"""
        tasks = []
        for i, contact in enumerate(contacts_batch):
            contact_index = start_index + i
            task = asyncio.create_task(self.process_contact_async(contact, contact_index))
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    
    async def process_contacts(self, contacts):
        """Process all contacts with optimized batching"""
        results = []
        
        # Process in batches for optimal performance
        for i in range(0, len(contacts), self.batch_size):
            batch = contacts[i:i+self.batch_size]
            batch_results = await self.process_batch(batch, i)
            results.extend(batch_results)
        
        return results

# Use our simplified scheduler
AsyncEmailProcessor.scheduler = SimpleEmailScheduler

# Global configuration
VERBOSE = False
DEBUG = False

def connect_to_db(db_path: str) -> sqlite3.Connection:
    """
    Connect to SQLite database and set row factory for dictionary results
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        SQLite connection object
    """
    if not os.path.exists(db_path):
        log(f"Database not found: {db_path}", always=True)
        sys.exit(1)
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        log(f"Error connecting to database {db_path}: {e}", always=True)
        sys.exit(1)

def get_organization_details(main_db_path: str, org_id: int) -> Dict[str, Any]:
    """
    Get organization details from the main database
    
    Args:
        main_db_path: Path to the main database
        org_id: Organization ID
        
    Returns:
        Organization details as a dictionary
    """
    log(f"Getting organization details for org_id: {org_id}", always=True)
    
    conn = connect_to_db(main_db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, turso_db_url, turso_auth_token FROM organizations WHERE id = ?", (org_id,))
        org = cursor.fetchone()
        
        if not org:
            log(f"Organization with ID {org_id} not found in the database", always=True)
            sys.exit(1)
            
        return dict(org)
    except sqlite3.Error as e:
        log(f"Error retrieving organization details: {e}", always=True)
        sys.exit(1)
    finally:
        conn.close()

def get_contacts_from_org_db(org_db_path: str, org_id: int) -> List[Dict[str, Any]]:
    """
    Get contacts from the organization's database
    
    Args:
        org_db_path: Path to the organization's database
        org_id: Organization ID
        
    Returns:
        List of contacts as dictionaries
        
    Raises:
        ValueError: If critical columns are missing
        sqlite3.Error: If database errors occur
    """
    log(f"Getting contacts from organization database: {org_db_path}", always=True)
    
    conn = connect_to_db(org_db_path)
    try:
        cursor = conn.cursor()
        
        # Check if the contacts table exists and has the required columns
        cursor.execute("PRAGMA table_info(contacts)")
        columns = [column['name'] for column in cursor.fetchall()]
        
        critical_columns = ['id', 'email']  # These must exist
        optional_columns = ['first_name', 'last_name', 'birth_date', 'state', 'effective_date', 'zip_code']
        
        missing_critical = [col for col in critical_columns if col not in columns]
        if missing_critical:
            raise ValueError(f"Missing critical columns in contacts table: {', '.join(missing_critical)}")
            
        missing_optional = [col for col in optional_columns if col not in columns]
        if missing_optional:
            log(f"Missing optional columns in contacts table: {', '.join(missing_optional)}", always=True)
        
        # Build query based on available columns
        select_parts = []
        
        # Handle ID column specially
        if 'id' in columns:
            select_parts.append('id')
        else:
            select_parts.append('rowid as id')
            
        # Add email (required)
        select_parts.append('email')
        
        # Add optional columns if they exist
        for col in optional_columns:
            if col in columns:
                select_parts.append(col)
                
        query = f"SELECT {', '.join(select_parts)} FROM contacts"
        cursor.execute(query)
        
        contacts = []
        for row in cursor.fetchall():
            contact = dict(row)
            contact['organization_id'] = org_id
            
            # Only set defaults for optional fields
            if 'first_name' not in contact:
                contact['first_name'] = None
            if 'last_name' not in contact:
                contact['last_name'] = None
            if 'birth_date' not in contact:
                contact['birth_date'] = None
            if 'effective_date' not in contact:
                contact['effective_date'] = None
            if 'zip_code' not in contact:
                contact['zip_code'] = None
                
            # Don't set a default state - we'll determine it from ZIP code later
            if 'state' not in contact:
                contact['state'] = None
                
            contacts.append(contact)
            
        log(f"Retrieved {len(contacts)} contacts from organization database", always=True)
        return contacts
    except sqlite3.Error as e:
        log(f"Error retrieving contacts: {e}", always=True)
        raise
    finally:
        conn.close()

def format_contact_data(contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Format contact data for compatibility with the email scheduler
    
    Args:
        contacts: List of contacts from the database
        
    Returns:
        Formatted contact data ready for scheduling
    """
    log("Formatting contact data for scheduler", always=True)
    
    formatted_contacts = []
    for contact in contacts:
        # Determine state from ZIP code if not already set
        state = contact.get('state')
        if not state and contact.get('zip_code'):
            from app import get_state_from_zip
            state = get_state_from_zip(contact['zip_code'])
        
        # Default to CA only if we couldn't determine state from ZIP
        if not state:
            log(f"No state or valid ZIP code for contact {contact.get('id')}, defaulting to CA", always=False)
            state = 'CA'
        
        # Ensure required fields exist
        formatted_contact = {
            'id': contact.get('id'),
            'contact_id': str(contact.get('id')),
            'first_name': contact.get('first_name', 'Unknown'),
            'last_name': contact.get('last_name', 'Unknown'),
            'email': contact.get('email', f"contact{contact.get('id')}@example.com"),
            'birth_date': contact.get('birth_date'),
            'effective_date': contact.get('effective_date'),
            'state': state,
            'organization_id': contact.get('organization_id')
        }
        
        # Skip contacts with missing critical data
        if not formatted_contact['birth_date'] and not formatted_contact['effective_date']:
            log(f"Skipping contact {formatted_contact['id']}: Missing both birth_date and effective_date", always=False)
            continue
            
        # Convert date fields if needed
        for date_field in ['birth_date', 'effective_date']:
            if formatted_contact[date_field] and not isinstance(formatted_contact[date_field], date) and not isinstance(formatted_contact[date_field], str):
                formatted_contact[date_field] = formatted_contact[date_field].isoformat()
                
        formatted_contacts.append(formatted_contact)
        
    log(f"Formatted {len(formatted_contacts)} contacts for scheduling", always=True)
    return formatted_contacts

def generate_link(org_id: int, contact_id: str, email_type: str, email_date: str) -> str:
    """
    Generate a tracking link for the email using quote ID system
    
    Args:
        org_id: Organization ID
        contact_id: Contact ID
        email_type: Type of email (birthday, effective_date, aep, post_window)
        email_date: Scheduled date for the email
        
    Returns:
        Generated URL for tracking
    """
    import hashlib
    import os

    # Convert contact_id to int for quote ID generation
    contact_id_int = int(contact_id)
    
    # Get quote secret from environment with default fallback
    quote_secret = os.environ.get('QUOTE_SECRET', 'your-default-secret-key')
    
    # Create data string to hash - EXACTLY matching TypeScript implementation
    # Convert numbers to strings first to ensure exact string concatenation
    org_id_str = str(org_id)
    contact_id_str = str(contact_id_int)
    data_to_hash = f"{org_id_str}-{contact_id_str}-{quote_secret}"
    
    # Generate hash using hashlib - encode as UTF-8 to match Node.js behavior
    hash_value = hashlib.sha256(data_to_hash.encode('utf-8')).hexdigest()[:8]
    
    # Combine components into quote ID
    quote_id = f"{org_id}-{contact_id_int}-{hash_value}"
    
    # Get base URL from environment or use default
    base_url = os.environ.get('EMAIL_SCHEDULER_BASE_URL', 'https://maxretain.com')
    
    # Ensure quote ID is properly URL encoded
    from urllib.parse import quote
    quote_id_enc = quote(quote_id)
    
    # Construct tracking URL with quote ID
    return f"{base_url.rstrip('/')}/compare?id={quote_id_enc}"

def write_results_to_csv(results: List[Dict[str, Any]], contacts: List[Dict[str, Any]], 
                         org_id: int, output_csv: str) -> None:
    """
    Write scheduling results to CSV
    
    Args:
        results: Results from the email scheduler
        contacts: Original contact data
        org_id: Organization ID
        output_csv: Path to the output CSV file
    """
    log(f"Writing results to CSV: {output_csv}", always=True)
    
    # Create a lookup dictionary for contacts
    contact_dict = {str(contact['id']): contact for contact in contacts}
    
    # Prepare data for CSV
    csv_data = []
    
    for result in results:
        contact_id = result['contact_id']
        contact = contact_dict.get(contact_id, {})
        
        # Process scheduled emails
        for email in result.get('emails', []):
            email_type = email.get('type', '')
            email_date = email.get('date', '')
            
            row = {
                'org_id': org_id,
                'contact_id': contact_id,
                'email': contact.get('email', f"contact{contact_id}@example.com"),
                'first_name': contact.get('first_name', 'Unknown'),
                'last_name': contact.get('last_name', 'Unknown'),
                'state': contact.get('state', 'CA'),  # Add state to CSV output
                'birth_date': contact.get('birth_date', ''),
                'effective_date': contact.get('effective_date', ''),
                'email_type': email_type,
                'email_date': email_date,
                'link': generate_link(org_id, contact_id, email_type, email_date),
                'skipped': 'No',
                'reason': email.get('reason', '')
            }
            csv_data.append(row)
            
        # Process skipped emails
        for skipped in result.get('skipped', []):
            email_type = skipped.get('type', '')
            email_date = skipped.get('date', '')
            reason = skipped.get('reason', 'Unknown reason')
            
            row = {
                'org_id': org_id,
                'contact_id': contact_id,
                'email': contact.get('email', f"contact{contact_id}@example.com"),
                'first_name': contact.get('first_name', 'Unknown'),
                'last_name': contact.get('last_name', 'Unknown'),
                'state': contact.get('state', 'CA'),  # Add state to CSV output
                'birth_date': contact.get('birth_date', ''),
                'effective_date': contact.get('effective_date', ''),
                'email_type': f"{email_type} (skipped)",
                'email_date': email_date or '',
                'link': '',
                'skipped': 'Yes',
                'reason': reason
            }
            csv_data.append(row)
    
    # Create directory if it doesn't exist
    output_dir = os.path.dirname(output_csv)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Define column order to ensure birth_date and effective_date appear in a logical place
    columns = [
        'org_id', 'contact_id', 'email', 'first_name', 'last_name',
        'state',  # Add state to column order
        'birth_date', 'effective_date',  # Place dates together
        'email_type', 'email_date', 'link', 'skipped', 'reason'
    ]
    
    # Use pandas to write the CSV with specified column order
    try:
        df = pd.DataFrame(csv_data)
        df = df[columns]  # Reorder columns
        df.to_csv(output_csv, index=False)
        log(f"Successfully wrote {len(csv_data)} rows to {output_csv}", always=True)
    except Exception as e:
        log(f"Error writing CSV: {e}", always=True)
        sys.exit(1)

async def process_contacts_async(contacts: List[Dict[str, Any]], current_date: date, 
                                end_date: date) -> List[Dict[str, Any]]:
    """
    Process contacts asynchronously using the SimpleEmailScheduler
    
    Args:
        contacts: Formatted contact data
        current_date: Start date for scheduling
        end_date: End date for scheduling
        
    Returns:
        Scheduling results
    """
    log("Processing contacts asynchronously", always=True)
    
    # Initialize the async email processor with our custom scheduler
    processor = SimpleAsyncEmailProcessor(current_date, end_date, batch_size=100, max_workers=20)
    results = await processor.process_contacts(contacts)
    
    # Process post-window emails based on business rules
    for i, result in enumerate(results):
        if i < len(contacts):
            contact = contacts[i]
            state = contact.get('state', 'CA')
            
            # Check if state has specific post-window rules
            if state in BIRTHDAY_RULE_STATES or state in EFFECTIVE_DATE_RULE_STATES:
                has_post_window = any(email.get('type') == EMAIL_TYPE_POST_WINDOW for email in result.get('emails', []))
                
                if not has_post_window:
                    # Calculate post-window date based on state rules
                    window_after = None
                    if state in BIRTHDAY_RULE_STATES:
                        window_after = BIRTHDAY_RULE_STATES[state]["window_after"]
                    elif state in EFFECTIVE_DATE_RULE_STATES:
                        window_after = EFFECTIVE_DATE_RULE_STATES[state]["window_after"]
                        
                    if window_after is not None:
                        # Find the latest rule window end date
                        rule_windows = calculate_rule_windows(contact, [], [], current_date, end_date)
                        if rule_windows:
                            latest_window_end = max(end for _, end, _, _, _ in rule_windows)
                            post_date = latest_window_end + timedelta(days=window_after + 1)
                            
                            if current_date <= post_date <= end_date:
                                result['emails'].append({
                                    "type": EMAIL_TYPE_POST_WINDOW,
                                    "date": str(post_date),
                                    "reason": f"Post-window email based on {state} state rules"
                                })
    
    return results

def process_contacts_sync(contacts: List[Dict[str, Any]], current_date: date, 
                         end_date: date) -> List[Dict[str, Any]]:
    """
    Process contacts synchronously using the SimpleEmailScheduler
    
    Args:
        contacts: Formatted contact data
        current_date: Start date for scheduling
        end_date: End date for scheduling
        
    Returns:
        Scheduling results
    """
    log("Processing contacts synchronously", always=True)
    
    # Initialize the email scheduler with the date range
    scheduler = SimpleEmailScheduler(current_date, end_date)
    results = []
    
    # Process each contact
    for i, contact in enumerate(contacts):
        contact_id = str(contact['id'])
        try:
            # Process the contact using the scheduler
            result = scheduler.process_contact(contact, i)
            
            # Ensure contact_id is included
            if not result.get('contact_id'):
                result['contact_id'] = contact_id
                
            # Add to results
            results.append(result)
            
        except Exception as e:
            log(f"Error processing contact {contact_id}: {e}", always=True)
            # Add error result
            results.append({
                "contact_id": contact_id,
                "emails": [],
                "skipped": [{"type": "all", "reason": str(e)}]
            })
        
    return results

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description="Schedule emails for a specific organization")
    parser.add_argument("--org-id", type=int, required=True, help="Organization ID")
    parser.add_argument("--output-csv", required=True, help="Output CSV file path")
    parser.add_argument("--main-db", default="main.db", help="Path to the main SQLite database")
    parser.add_argument("--org-db-dir", default="org_dbs/", help="Directory containing organization-specific databases")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--async", action="store_true", help="Use asynchronous processing")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set global config
    global DEBUG, VERBOSE
    DEBUG = args.debug
    VERBOSE = args.verbose
    
    # Parse start date
    current_date = None
    if args.start_date:
        try:
            current_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError as e:
            log(f"Invalid start date format: {e}", always=True)
            log("Start date must be in YYYY-MM-DD format", always=True)
            sys.exit(1)
    else:
        current_date = date.today()
    
    # Set end date to two years from start date
    end_date = current_date + timedelta(days=730)  # 2 years (365 * 2 + leap days)
    
    # Get organization details
    org = get_organization_details(args.main_db, args.org_id)
    log(f"Processing organization: {org['name']} (ID: {org['id']})", always=True)
    
    # Get contacts from organization database
    org_db_path = os.path.join(args.org_db_dir, f"org-{args.org_id}.db")
    contacts = get_contacts_from_org_db(org_db_path, args.org_id)
    
    # Format contact data for the scheduler
    formatted_contacts = format_contact_data(contacts)
    
    if not formatted_contacts:
        log("No valid contacts found for scheduling", always=True)
        sys.exit(1)
    
    # Process contacts
    try:
        log(f"Scheduling emails for {len(formatted_contacts)} contacts from {current_date} to {end_date}", always=True)
        
        results = []
        
        if getattr(args, 'async'):
            # Run asynchronously using the SimpleEmailScheduler
            results = asyncio.run(process_contacts_async(formatted_contacts, current_date, end_date))
        else:
            # Run synchronously using the SimpleEmailScheduler
            results = process_contacts_sync(formatted_contacts, current_date, end_date)
            
        # Count scheduled emails
        scheduled_count = sum(len(result.get('emails', [])) for result in results)
        skipped_count = sum(len(result.get('skipped', [])) for result in results)
        
        log(f"Scheduled {scheduled_count} emails, skipped {skipped_count} emails", always=True)
        
        # Write results to CSV
        write_results_to_csv(results, formatted_contacts, args.org_id, args.output_csv)
        
        log("Email scheduling completed successfully", always=True)
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        log(f"Error scheduling emails: {e}", always=True)
        log(f"Error details:\n{error_trace}", always=True)
        sys.exit(1)

if __name__ == "__main__":
    main()