#!/usr/bin/env python3
"""
Test script for email scheduler that loads real data from the database
and runs the scheduler on a random sample of contacts.
"""

import argparse
import json
import random
from datetime import date, datetime, timedelta
import os
import sys
from typing import List, Dict, Any, Optional
import logging

from email_scheduler_optimized import (
    EmailScheduler, 
    AsyncEmailProcessor,
    main_async,
    main_sync
)
from contact_rule_engine import ContactRuleEngine
from org_utils import (
    get_organization_details, 
    get_contacts_from_org_db, 
    format_contact_data
)

# Configure logging
logger = logging.getLogger(__name__)

def load_org_contacts(org_id: int, state: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load contacts from organization database"""
    # Set up paths
    main_db = "main.db"
    org_db_dir = "org_dbs"
    org_db_path = os.path.join(org_db_dir, f"org-{org_id}.db")
    
    # Get organization details
    org = get_organization_details(main_db, org_id)
    logger.info(f"Loading contacts for organization: {org['name']} (ID: {org_id})")
    
    # Get contacts from organization database
    contacts = get_contacts_from_org_db(org_db_path, org_id)
    formatted_contacts = format_contact_data(contacts)
    
    if state:
        # Filter contacts by state
        formatted_contacts = [c for c in formatted_contacts if c.get('state') == state]
        logger.info(f"Filtered to {len(formatted_contacts)} contacts from {state}")
    
    return formatted_contacts

def sample_contacts(contacts: List[Dict[str, Any]], sample_size: int) -> List[Dict[str, Any]]:
    """Randomly sample N contacts from the list"""
    if sample_size >= len(contacts):
        return contacts
    return random.sample(contacts, sample_size)

def main():
    parser = argparse.ArgumentParser(description="Test Email Scheduler with Organization Data")
    parser.add_argument("org_id", type=int, help="Organization ID to load contacts from")
    parser.add_argument("--num-contacts", "-n", type=int, default=5, 
                       help="Number of contacts to sample (default: 5)")
    parser.add_argument("--state", "-s", help="Filter contacts by state code (e.g. CA)")
    parser.add_argument("--output", "-o", help="Output JSON file for results")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--display", "-d", type=int, default=5,
                       help="Number of results to display (default: 5)")
    parser.add_argument("--use-async", action="store_true", help="Use async processing")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Configure logging based on debug/verbose settings
    log_level = logging.DEBUG if args.debug else (logging.INFO if args.verbose else logging.WARNING)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Set date range
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
            
        end_date = None
        if args.end_date:
            try:
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
            except ValueError as e:
                logger.error(f"Invalid end date format: {e}")
                logger.error("End date must be in YYYY-MM-DD format")
                sys.exit(1)
        else:
            end_date = current_date + timedelta(days=365)
        
        # Load and sample contacts
        all_contacts = load_org_contacts(args.org_id, args.state)
        if not all_contacts:
            logger.error("No contacts found!")
            sys.exit(1)
            
        logger.info(f"Sampling {args.num_contacts} contacts from {len(all_contacts)} total contacts")
        sampled_contacts = sample_contacts(all_contacts, args.num_contacts)
        
        # Process contacts
        if args.use_async:
            import asyncio
            logger.info("Processing contacts asynchronously...")
            results = asyncio.run(main_async(sampled_contacts, current_date, end_date))
        else:
            logger.info("Processing contacts synchronously...")
            results = main_sync(sampled_contacts, current_date, end_date)
        
        # Display results
        if args.display > 0:
            display_results(results, args.display)
        
        # Write results to file if specified
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Full results written to {args.output}")
        
        # Print summary
        total_emails = sum(len(r['emails']) for r in results)
        total_skipped = sum(len(r['skipped']) for r in results)
        print(f"\nSummary:")
        print(f"- Total contacts processed: {len(sampled_contacts)}")
        print(f"- Total emails scheduled: {total_emails}")
        print(f"- Total emails skipped: {total_skipped}")
        print(f"- Average emails per contact: {total_emails/len(sampled_contacts):.1f}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        if args.debug:
            import traceback
            logger.error(f"Error details:\n{traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main() 