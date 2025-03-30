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
        rule_windows: List of tuples (start_date, end_date, rule_type, state)
        end_date: End date for scheduling window
        
    Returns:
        List of post-window dates
    """
    post_window_dates = []
    for start, end, rule_type, state in rule_windows:
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

def calculate_birthday_email_date(birthday_date, email_year):
    """Calculate the date to send a birthday email (14 days before birthday)"""
    return birthday_date - timedelta(days=14)

def calculate_effective_date_email(effective_date, current_date):
    """Calculate the date to send an effective date email (30 days before)"""
    return effective_date - timedelta(days=30)

# Helper function to check if a date is within an exclusion period
def is_date_excluded(date_obj, exclusions, email_type=None, state=None):
    """
    Check if a date is in an exclusion period
    
    Args:
        date_obj: The date to check
        exclusions: List of exclusion periods
        email_type: Type of email (to apply special rules)
        state: State code (to apply special rules)
        
    Returns:
        True if date is excluded, False otherwise
    """
    # If exclusion list is empty (for states without rule windows like Kansas), 
    # always return False - no exclusions apply
    if not exclusions:
        return False
        
    # Special bypass rules:
    # 1. For birthday emails in non-special rule states, bypass exclusion checks
    if email_type == "birthday" and state and state not in BIRTHDAY_RULE_STATES:
        return False
        
    # 2. For effective date emails in special rule states, bypass exclusion checks
    if email_type == "effective_date" and state and state in EFFECTIVE_DATE_RULE_STATES:
        return False
    
    # Normal exclusion check
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
        
        # Validator for scheduled emails
        self.validator = type('MockValidator', (), {
            'validate_scheduled_emails': lambda *args: True,
            'validate_exclusions': lambda *args: True
        })()
    
    def _precompute_aep_dates(self):
        """Precompute AEP dates for the date range"""
        aep_dates = {}
        for yr in range(self.current_date.year, self.end_date.year + 1):
            # Default AEP dates - distribute across August and September
            dates = []
            for month in [8, 9]:  # August and September
                for week in range(4):  # 4 weeks per month
                    # Calculate date for this week (every Thursday)
                    week_date = date(yr, month, 1)
                    while week_date.weekday() != 3:  # 3 = Thursday
                        week_date += timedelta(days=1)
                    week_date += timedelta(weeks=week)
                    if week_date.month == month:  # Only add if still in target month
                        dates.append(week_date)
            aep_dates[yr] = dates
        return aep_dates
    
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
            
            # Calculate rule windows and exclusion periods
            rule_windows = calculate_rule_windows(contact, [], [], self.current_date, self.end_date)
            
            # Get state and check if it's year-round
            state = contact.get('state', 'CA')
            
            # Skip only if it's a year-round enrollment state
            if state in YEAR_ROUND_ENROLLMENT_STATES:
                # Skip all emails for year-round enrollment states
                for email_type in [EMAIL_TYPE_BIRTHDAY, EMAIL_TYPE_EFFECTIVE_DATE, EMAIL_TYPE_AEP, EMAIL_TYPE_POST_WINDOW]:
                    result["skipped"].append({
                        "type": email_type,
                        "reason": "Year-round enrollment state"
                    })
                return result
            
            # Get latest window end date for post-window calculations (if there are any rule windows)
            latest_window_end = None
            if rule_windows:
                latest_window_end = max(end for _, end, _, _ in rule_windows)
            
            # Calculate exclusion periods
            exclusion_periods = calculate_exclusion_periods(rule_windows, self.current_date, self.end_date)
            
            # Process birthdays
            birthday_scheduled = False
            if contact.get('birth_date'):
                try:
                    # Get actual birthdate
                    if isinstance(contact['birth_date'], str):
                        birthday = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date()
                    elif isinstance(contact['birth_date'], date):
                        birthday = contact['birth_date']
                    else:
                        log(f"Invalid birth_date format for contact {contact['id']}: {contact['birth_date']}", always=True)
                        result["skipped"].append({
                            "type": EMAIL_TYPE_BIRTHDAY,
                            "reason": "Invalid birth_date format"
                        })
                        birthday = None
                    
                    if birthday:
                        # For IL residents, check age before scheduling birthday email
                        if state == "IL":
                            # Calculate age
                            age = self.current_date.year - birthday.year
                            if self.current_date.month < birthday.month or (self.current_date.month == birthday.month and self.current_date.day < birthday.day):
                                age -= 1
                                
                            # Skip birthday emails for IL residents 76+ years old
                            if age >= 76:
                                result["skipped"].append({
                                    "type": EMAIL_TYPE_BIRTHDAY,
                                    "reason": "Illinois resident over 76 years old"
                                })
                                birthday = None

                        if birthday:
                            # Calculate all birthdays in our date range
                            birthdays = []
                            # Current year birthday if not already passed
                            if (birthday.month > self.current_date.month or 
                                (birthday.month == self.current_date.month and birthday.day >= self.current_date.day)):
                                birthdays.append(date(self.current_date.year, birthday.month, birthday.day))
                            
                            # Add birthdays for future years
                            for yr in range(self.current_date.year + 1, self.end_date.year + 1):
                                if birthday.month == 2 and birthday.day == 29 and not is_leap_year(yr):
                                    birthdays.append(date(yr, 2, 28))
                                else:
                                    birthdays.append(date(yr, birthday.month, birthday.day))
                            
                            # Schedule birthday emails (14 days before each birthday)
                            for birthday_date in birthdays:
                                email_date = calculate_birthday_email_date(birthday_date, birthday_date.year)
                                
                                # Only schedule if email date is within our range and not excluded
                                if self.current_date <= email_date <= self.end_date:
                                    # For birthday emails, bypass exclusion for non-special rule states
                                    # This ensures that non-special states like Alaska still get birthday emails
                                    bypass_exclusion = state not in BIRTHDAY_RULE_STATES
                                    
                                    # Check if date would be excluded by normal rules
                                    would_be_excluded = is_date_excluded(email_date, exclusion_periods, "birthday", state)
                                    
                                    # Log exclusion status for debugging
                                    if would_be_excluded:
                                        log(f"Birthday email for contact {contact['id']} on {email_date} would be excluded, bypass_exclusion={bypass_exclusion}", always=DEBUG)
                                    
                                    if bypass_exclusion or not would_be_excluded:
                                        log(f"Scheduling birthday email for contact {contact['id']} (state={state}) on {email_date}", always=DEBUG)
                                        result["emails"].append({
                                            "type": "birthday",  # Ensure lowercase consistent with frontend
                                            "date": email_date.isoformat()
                                        })
                                        birthday_scheduled = True
                                    else:
                                        log(f"Skipping birthday email for contact {contact['id']} on {email_date} due to exclusion period", always=DEBUG)
                            # If no birthdays were scheduled, provide a reason
                            if not birthday_scheduled:
                                result["skipped"].append({
                                    "type": "birthday",
                                    "reason": "No birthdays in scheduling window or all dates excluded"
                                })
                except Exception as e:
                    log(f"Error processing birthdate for contact {contact['id']}: {e}", always=True)
                    result["skipped"].append({
                        "type": "birthday",
                        "reason": f"Error processing birth_date: {str(e)}"
                    })
            else:
                # Skip birthday emails if no birth date
                result["skipped"].append({
                    "type": "birthday",
                    "reason": "No birth date provided"
                })
            
            # Process effective dates
            effective_date_scheduled = False
            if contact.get('effective_date'):
                try:
                    # Get actual effective date
                    if isinstance(contact['effective_date'], str):
                        effective_date = datetime.strptime(contact['effective_date'], "%Y-%m-%d").date()
                    elif isinstance(contact['effective_date'], date):
                        effective_date = contact['effective_date']
                    else:
                        log(f"Invalid effective_date format for contact {contact['id']}: {contact['effective_date']}", always=True)
                        result["skipped"].append({
                            "type": "effective_date",
                            "reason": "Invalid effective_date format"
                        })
                        effective_date = None
                    
                    if effective_date:
                        # Calculate all effective dates in our date range
                        effective_dates = []
                        # Start with current year if not already passed
                        if (effective_date.month > self.current_date.month or 
                            (effective_date.month == self.current_date.month and effective_date.day >= self.current_date.day)):
                            effective_dates.append(date(self.current_date.year, effective_date.month, effective_date.day))
                        
                        # Add effective dates for future years
                        for yr in range(self.current_date.year + 1, self.end_date.year + 1):
                            effective_dates.append(date(yr, effective_date.month, effective_date.day))
                        
                        # Schedule effective date emails (30 days before each date)
                        for eff_date in effective_dates:
                            email_date = calculate_effective_date_email(eff_date, self.current_date)
                            
                            # Only schedule if email date is within our range and not excluded
                            if self.current_date <= email_date <= self.end_date:
                                # For effective date emails, bypass exclusion for special rule states like Missouri
                                # This ensures that states like Missouri still get effective date emails
                                bypass_exclusion = state in EFFECTIVE_DATE_RULE_STATES
                                
                                # Check if date would be excluded by normal rules
                                would_be_excluded = is_date_excluded(email_date, exclusion_periods, "effective_date", state)
                                
                                # Log exclusion status for debugging
                                if would_be_excluded:
                                    log(f"Effective date email for contact {contact['id']} on {email_date} would be excluded, bypass_exclusion={bypass_exclusion}", always=DEBUG)
                                
                                if bypass_exclusion or not would_be_excluded:
                                    log(f"Scheduling effective date email for contact {contact['id']} (state={state}) on {email_date}", always=DEBUG)
                                    result["emails"].append({
                                        "type": "effective_date",  # Ensure lowercase consistent with frontend
                                        "date": email_date.isoformat()
                                    })
                                    effective_date_scheduled = True
                                else:
                                    log(f"Skipping effective date email for contact {contact['id']} on {email_date} due to exclusion period", always=DEBUG)
                        # If no effective dates were scheduled, provide a reason
                        if not effective_date_scheduled:
                            result["skipped"].append({
                                "type": "effective_date",
                                "reason": "No effective dates in scheduling window or all dates excluded"
                            })
                except Exception as e:
                    log(f"Error processing effective date for contact {contact['id']}: {e}", always=True)
                    result["skipped"].append({
                        "type": "effective_date",
                        "reason": f"Error processing effective_date: {str(e)}"
                    })
            else:
                # Skip effective date emails if no effective date
                result["skipped"].append({
                    "type": "effective_date",
                    "reason": "No effective date provided"
                })
            
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
                    if not is_date_excluded(aep_date, exclusion_periods, "aep", state):
                        result["emails"].append({
                            "type": "aep",  # Ensure lowercase consistent with frontend
                            "date": str(aep_date)
                        })
                        aep_scheduled = True
                        break  # Only schedule one AEP email per contact
            
            if not aep_scheduled:
                result["skipped"].append({
                    "type": "aep",
                    "reason": "No suitable AEP date found"
                })
            
            # Schedule post-window emails
            post_window_scheduled = False
            post_window_dates = calculate_post_window_dates(rule_windows, self.end_date)
            if post_window_dates:
                for post_date in sorted(post_window_dates):
                    if post_date >= self.current_date and post_date <= self.end_date:
                        # Post-window emails bypass exclusion checks
                        result["emails"].append({
                            "type": "post_window",  # Ensure lowercase consistent with frontend
                            "date": str(post_date)
                        })
                        post_window_scheduled = True
                        break  # Only schedule one post-window email
            
            if not post_window_scheduled:
                result["skipped"].append({
                    "type": "post_window",
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

def parse_date_flexible(date_str: str) -> Optional[date]:
    """
    Parse a date string flexibly, handling both dash and slash formats.
    Supports formats: YYYY-MM-DD, YYYY/MM/DD, MM/DD/YYYY, MM-DD-YYYY
    
    Args:
        date_str: Date string to parse
        
    Returns:
        datetime.date object if successful, None if parsing fails
    """
    if not date_str:
        return None
        
    formats = [
        "%Y-%m-%d",  # YYYY-MM-DD
        "%Y/%m/%d",  # YYYY/MM/DD
        "%m/%d/%Y",  # MM/DD/YYYY
        "%m-%d-%Y"   # MM-DD-YYYY
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    return None

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
        # Always try to determine state from ZIP code first
        state = None
        if contact.get('zip_code'):
            from app import get_state_from_zip
            state = get_state_from_zip(contact['zip_code'])
        
        # If we couldn't get state from ZIP, check if existing state is valid
        if not state and contact.get('state'):
            from app import ALL_STATES
            if contact['state'] in ALL_STATES:
                state = contact['state']
        
        # Default to CA if we still don't have a valid state
        if not state:
            log(f"Could not determine valid state for contact {contact.get('id')}, defaulting to CA", always=False)
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
            if formatted_contact[date_field]:
                if not isinstance(formatted_contact[date_field], date):
                    if isinstance(formatted_contact[date_field], str):
                        parsed_date = parse_date_flexible(formatted_contact[date_field])
                        if parsed_date:
                            formatted_contact[date_field] = parsed_date.isoformat()
                        else:
                            log(f"Warning: Could not parse {date_field} for contact {formatted_contact['id']}: {formatted_contact[date_field]}", always=True)
                            formatted_contact[date_field] = None
                    else:
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
    
    # Prepare data for CSV, using a set to track unique entries
    seen_entries = set()  # Track unique entries to prevent duplicates
    csv_data = []
    
    for result in results:
        contact_id = result['contact_id']
        contact = contact_dict.get(contact_id, {})
        
        # Process scheduled emails
        for email in result.get('emails', []):
            email_type = email.get('type', '')
            # Ensure date is a string
            email_date = str(email.get('date', ''))
            
            # Create a unique key for this email entry
            unique_key = f"{contact_id}-{email_type}-{email_date}"
            if unique_key in seen_entries:
                continue
            seen_entries.add(unique_key)
            
            # Generate complete link
            link = generate_link(org_id, contact_id, email_type, email_date)
            
            row = {
                'org_id': org_id,
                'contact_id': contact_id,
                'email': contact.get('email', f"contact{contact_id}@example.com"),
                'first_name': contact.get('first_name', 'Unknown'),
                'last_name': contact.get('last_name', 'Unknown'),
                'state': contact.get('state', 'CA'),
                'birth_date': contact.get('birth_date', ''),
                'effective_date': contact.get('effective_date', ''),
                'email_type': email_type,
                'email_date': email_date,
                'link': link,
                'skipped': 'No',
                'reason': email.get('reason', '')
            }
            csv_data.append(row)
            
        # Process skipped emails
        for skipped in result.get('skipped', []):
            email_type = skipped.get('type', 'all')
            reason = skipped.get('reason', 'Unknown reason')
            
            # Create a unique key for this skipped entry
            unique_key = f"{contact_id}-{email_type}-skipped-{reason}"
            if unique_key in seen_entries:
                continue
            seen_entries.add(unique_key)
            
            row = {
                'org_id': org_id,
                'contact_id': contact_id,
                'email': contact.get('email', f"contact{contact_id}@example.com"),
                'first_name': contact.get('first_name', 'Unknown'),
                'last_name': contact.get('last_name', 'Unknown'),
                'state': contact.get('state', 'CA'),
                'birth_date': contact.get('birth_date', ''),
                'effective_date': contact.get('effective_date', ''),
                'email_type': email_type,
                'email_date': '',  # Empty string for skipped emails
                'link': '',  # No link for skipped emails
                'skipped': 'Yes',
                'reason': reason
            }
            csv_data.append(row)
    
    # Define column order
    columns = [
        'org_id', 'contact_id', 'email', 'first_name', 'last_name',
        'state', 'birth_date', 'effective_date',
        'email_type', 'email_date', 'link', 'skipped', 'reason'
    ]
    
    # Use pandas to write the CSV with specified column order
    try:
        df = pd.DataFrame(csv_data)
        df = df[columns]  # Reorder columns
        
        # Replace any remaining nan values with empty strings
        df = df.fillna('')
        
        # Convert all date fields to strings to ensure consistent format
        if 'email_date' in df.columns:
            df['email_date'] = df['email_date'].astype(str)
        if 'birth_date' in df.columns:
            df['birth_date'] = df['birth_date'].astype(str)
        if 'effective_date' in df.columns:
            df['effective_date'] = df['effective_date'].astype(str)
            
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
            
            # Post-window emails should only be scheduled for states with specific rules
            # This is already handled by the calculate_rule_windows function which will
            # return an empty list for states without specific rules
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
                            latest_window_end = max(end for _, end, _, _ in rule_windows)
                            post_date = latest_window_end + timedelta(days=window_after + 1)
                            
                            if current_date <= post_date <= end_date:
                                result['emails'].append({
                                    "type": EMAIL_TYPE_POST_WINDOW,
                                    "date": str(post_date),
                                    "reason": f"Post-window email based on {state} state rules"
                                })
            else:
                # For states without specific rules, ensure there are no post-window emails
                # and add a skip reason if needed
                has_skip_reason = any(skip.get('type') == EMAIL_TYPE_POST_WINDOW for skip in result.get('skipped', []))
                if not has_skip_reason:
                    result.setdefault('skipped', []).append({
                        "type": EMAIL_TYPE_POST_WINDOW,
                        "reason": f"No post-window emails for state {state} - no rule windows"
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
            
            # Apply the same post-window email logic as in the async processor
            state = contact.get('state', 'CA')
            
            # Post-window emails should only be scheduled for states with specific rules
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
                            latest_window_end = max(end for _, end, _, _ in rule_windows)
                            post_date = latest_window_end + timedelta(days=window_after + 1)
                            
                            if current_date <= post_date <= end_date:
                                result['emails'].append({
                                    "type": EMAIL_TYPE_POST_WINDOW,
                                    "date": str(post_date),
                                    "reason": f"Post-window email based on {state} state rules"
                                })
            else:
                # For states without specific rules, ensure there are no post-window emails
                # and add a skip reason if needed
                has_skip_reason = any(skip.get('type') == EMAIL_TYPE_POST_WINDOW for skip in result.get('skipped', []))
                if not has_skip_reason:
                    result.setdefault('skipped', []).append({
                        "type": EMAIL_TYPE_POST_WINDOW,
                        "reason": f"No post-window emails for state {state} - no rule windows"
                    })
                
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
    parser.add_argument("--debug", action="store_true", help="Enable debug logging", default=True)
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging", default=True)
    
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