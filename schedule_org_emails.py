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
        --main-db FILE          Path to the main SQLite database (default: main.db)
        --org-db-dir DIR        Directory containing organization-specific databases (default: org_dbs/)
        --start-date YYYY-MM-DD Start date for scheduling (default: today)
        --use-async             Use asynchronous processing (faster for large datasets)
        --debug                 Enable debug logging
        --verbose              Enable verbose logging
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

# Import our optimized scheduler components
from email_scheduler_optimized import (
    EmailScheduler,
    AsyncEmailProcessor,
    main_async,
    main_sync
)
from contact_rule_engine import ContactRuleEngine

# Import from email_scheduler_common
from email_scheduler_common import (
    get_state_from_zip,
    ALL_STATES,
    logger
)

# Configure logging
import logging
logger = logging.getLogger(__name__)

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
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database {db_path}: {e}")
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
    logger.info(f"Getting organization details for org_id: {org_id}")
    
    conn = connect_to_db(main_db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, turso_db_url, turso_auth_token FROM organizations WHERE id = ?", (org_id,))
        org = cursor.fetchone()
        
        if not org:
            logger.error(f"Organization with ID {org_id} not found in the database")
            sys.exit(1)
            
        return dict(org)
    except sqlite3.Error as e:
        logger.error(f"Error retrieving organization details: {e}")
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
    """
    logger.info(f"Getting contacts from organization database: {org_db_path}")
    
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
            logger.warning(f"Missing optional columns in contacts table: {', '.join(missing_optional)}")
        
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
            
        logger.info(f"Retrieved {len(contacts)} contacts from organization database")
        return contacts
    except sqlite3.Error as e:
        logger.error(f"Error retrieving contacts: {e}")
        raise
    finally:
        conn.close()

def parse_date_flexible(date_str: str) -> Optional[date]:
    """Parse a date string flexibly, handling both dash and slash formats"""
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
    """Format contact data for compatibility with the email scheduler"""
    logger.info("Formatting contact data for scheduler")
    
    formatted_contacts = []
    for contact in contacts:
        # Always try to determine state from ZIP code first
        state = None
        if contact.get('zip_code'):
            state = get_state_from_zip(contact['zip_code'])
        
        # If we couldn't get state from ZIP, check if existing state is valid
        if not state and contact.get('state'):
            if contact['state'] in ALL_STATES:
                state = contact['state']
        
        # Default to CA if we still don't have a valid state
        if not state:
            logger.warning(f"Could not determine valid state for contact {contact.get('id')}, defaulting to CA")
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
            logger.warning(f"Skipping contact {formatted_contact['id']}: Missing both birth_date and effective_date")
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
                            logger.warning(f"Could not parse {date_field} for contact {formatted_contact['id']}: {formatted_contact[date_field]}")
                            formatted_contact[date_field] = None
                    else:
                        formatted_contact[date_field] = formatted_contact[date_field].isoformat()
                
        formatted_contacts.append(formatted_contact)
        
    logger.info(f"Formatted {len(formatted_contacts)} contacts for scheduling")
    return formatted_contacts

def generate_link(org_id: int, contact_id: str, email_type: str, email_date: str) -> str:
    """Generate a tracking link for the email using quote ID system"""
    import hashlib
    import os

    # Convert contact_id to int for quote ID generation
    contact_id_int = int(contact_id)
    
    # Get quote secret from environment with default fallback
    quote_secret = os.environ.get('QUOTE_SECRET', 'your-default-secret-key')
    
    # Create data string to hash - EXACTLY matching TypeScript implementation
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
    """Write scheduling results to CSV"""
    logger.info(f"Writing results to CSV: {output_csv}")
    
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
        logger.info(f"Successfully wrote {len(csv_data)} rows to {output_csv}")
    except Exception as e:
        logger.error(f"Error writing CSV: {e}")
        sys.exit(1)

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description="Schedule emails for a specific organization")
    parser.add_argument("--org-id", type=int, required=True, help="Organization ID")
    parser.add_argument("--output-csv", required=True, help="Output CSV file path")
    parser.add_argument("--main-db", default="main.db", help="Path to the main SQLite database")
    parser.add_argument("--org-db-dir", default="org_dbs/", help="Directory containing organization-specific databases")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--use-async", action="store_true", help="Use asynchronous processing")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging", default=True)
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging", default=True)
    
    args = parser.parse_args()
    
    # Set global config
    global DEBUG, VERBOSE
    DEBUG = args.debug
    VERBOSE = args.verbose
    
    # Configure logging based on debug/verbose settings
    log_level = logging.DEBUG if DEBUG else (logging.INFO if VERBOSE else logging.WARNING)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Parse start date
        current_date = None
        if args.start_date:
            try:
                current_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            except ValueError as e:
                logger.error(f"Invalid start date format: {e}")
                logger.error("Start date must be in YYYY-MM-DD format")
                sys.exit(1)
        else:
            current_date = date.today()
        
        # Set end date to two years from start date
        end_date = current_date + timedelta(days=730)  # 2 years
        
        # Get organization details
        org = get_organization_details(args.main_db, args.org_id)
        logger.info(f"Processing organization: {org['name']} (ID: {org['id']})")
        
        # Get contacts from organization database
        org_db_path = os.path.join(args.org_db_dir, f"org-{args.org_id}.db")
        contacts = get_contacts_from_org_db(org_db_path, args.org_id)
        
        # Format contact data for the scheduler
        formatted_contacts = format_contact_data(contacts)
        
        if not formatted_contacts:
            logger.error("No valid contacts found for scheduling")
            sys.exit(1)
        
        # Process contacts
        logger.info(f"Scheduling emails for {len(formatted_contacts)} contacts from {current_date} to {end_date}")
        
        if args.use_async:
            # Process asynchronously
            logger.info("Processing contacts asynchronously...")
            results = asyncio.run(main_async(formatted_contacts, current_date, end_date))
        else:
            # Process synchronously
            logger.info("Processing contacts synchronously...")
            results = main_sync(formatted_contacts, current_date, end_date)
        
        # Count scheduled emails
        scheduled_count = sum(len(result.get('emails', [])) for result in results)
        skipped_count = sum(len(result.get('skipped', [])) for result in results)
        
        logger.info(f"Scheduled {scheduled_count} emails, skipped {skipped_count} emails")
        
        # Write results to CSV
        write_results_to_csv(results, formatted_contacts, args.org_id, args.output_csv)
        
        logger.info("Email scheduling completed successfully")
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error scheduling emails: {e}")
        logger.error(f"Error details:\n{error_trace}")
        sys.exit(1)

if __name__ == "__main__":
    main()