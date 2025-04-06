"""
Main script for sending scheduled emails.
Reads scheduled emails from the output JSON and sends them via SendGrid.
"""

import os
import json
import argparse
from datetime import date, datetime, timedelta
import time
from typing import Dict, List, Any, Optional

from email_scheduler_common import logger
from sendgrid_client import SendGridClient
from email_template_engine import EmailTemplateEngine

# Initialize the template engine
template_engine = EmailTemplateEngine()

def get_email_content(email_type, contact, email_date):
    """Get email content using the template engine"""
    return template_engine.render_email(email_type, contact, email_date)

def get_email_html_content(email_type, contact, email_date):
    """Get HTML email content using the template engine"""
    result = template_engine.render_email(email_type, contact, email_date, html=True)
    return result['html']  # Return just the HTML string, not the whole dictionary

def load_scheduled_emails(input_file: str) -> List[Dict[str, Any]]:
    """Load scheduled emails from JSON file"""
    try:
        with open(input_file, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Error loading scheduled emails from {input_file}: {e}")
        return []

def load_contact_details(contact_id: str, contacts_file: str) -> Optional[Dict[str, Any]]:
    """Load contact details from contacts file"""
    try:
        with open(contacts_file, 'r') as f:
            contacts = json.load(f)
        
        # Find the contact by ID
        for contact in contacts:
            if str(contact.get('id')) == str(contact_id):
                return contact
        
        logger.error(f"Contact {contact_id} not found in contacts file")
        return None
    except Exception as e:
        logger.error(f"Error loading contact details from {contacts_file}: {e}")
        return None

def send_scheduled_emails(
    scheduled_data: List[Dict[str, Any]], 
    contacts_file: str,
    dry_run: bool = True,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: Optional[int] = None,
    delay: float = 0.0
):
    """Send scheduled emails using SendGrid"""
    # Initialize the SendGrid client
    client = SendGridClient(dry_run=dry_run)
    
    # Default to today if no start date provided
    if start_date is None:
        start_date = date.today()
    
    # Default to one year from start date if no end date provided
    if end_date is None:
        end_date = start_date + timedelta(days=365)
    
    # Track stats
    total_emails = 0
    successful_emails = 0
    failed_emails = 0
    
    # Process each contact's scheduled emails
    for contact_data in scheduled_data:
        contact_id = contact_data.get('contact_id')
        scheduled_emails = contact_data.get('emails', [])
        
        if not contact_id or not scheduled_emails:
            continue
        
        # Load contact details
        contact = load_contact_details(contact_id, contacts_file)
        if not contact:
            logger.warning(f"Skipping emails for contact {contact_id}: Contact details not found")
            continue
        
        # Ensure contact has an email address
        if not contact.get('email'):
            logger.warning(f"Skipping emails for contact {contact_id}: No email address")
            continue
        
        to_email = contact['email']
        
        # Process scheduled emails for this contact
        for email in scheduled_emails:
            email_type = email.get('type')
            email_date_str = email.get('date')
            
            if not email_type or not email_date_str:
                continue
            
            # Parse the email date
            try:
                email_date = datetime.strptime(email_date_str, "%Y-%m-%d").date()
            except:
                logger.error(f"Invalid date format for email: {email_date_str}")
                continue
            
            # Skip emails outside our date range
            if email_date < start_date or email_date > end_date:
                continue
            
            # Generate email content
            try:
                content = get_email_content(email_type, contact, email_date)
                html_content = get_email_html_content(email_type, contact, email_date)
                
                # Send the email
                total_emails += 1
                result = client.send_email(
                    to_email=to_email,
                    subject=content['subject'],
                    content=content['body'],
                    html_content=html_content,  # Already fixed by modifying get_email_html_content
                    dry_run=dry_run
                )
                
                if result:
                    successful_emails += 1
                    logger.info(f"Email {email_type} for contact {contact_id} scheduled on {email_date_str} sent successfully")
                else:
                    failed_emails += 1
                    logger.error(f"Failed to send {email_type} email for contact {contact_id} scheduled on {email_date_str}")
                
                # Add a delay if specified (helps with rate limits)
                if delay > 0 and total_emails < len(scheduled_data):
                    time.sleep(delay)
                
                # Check if we've hit the limit
                if limit and total_emails >= limit:
                    logger.info(f"Reached email limit of {limit}, stopping")
                    break
                
            except Exception as e:
                logger.error(f"Error sending {email_type} email for contact {contact_id}: {e}")
                failed_emails += 1
        
        # Check if we've hit the limit
        if limit and total_emails >= limit:
            break
    
    # Log summary
    logger.info(f"Email sending complete: {successful_emails} successful, {failed_emails} failed, {total_emails} total")
    
    return {
        "total": total_emails,
        "successful": successful_emails,
        "failed": failed_emails
    }

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description="Send scheduled emails using SendGrid")
    parser.add_argument("--input", required=True, help="Input JSON file with scheduled emails")
    parser.add_argument("--contacts", required=True, help="JSON file with contact details")
    parser.add_argument("--start-date", help="Start date for emails (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for emails (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="Maximum number of emails to send")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay between emails in seconds")
    parser.add_argument("--live", action="store_true", help="Send actual emails (default is dry-run)")
    
    args = parser.parse_args()
    
    # Parse dates if provided
    start_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    
    end_date = None
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    # Determine dry_run mode (default to True - dry run)
    dry_run = not args.live
    
    # Load scheduled emails
    scheduled_data = load_scheduled_emails(args.input)
    
    # Send emails
    mode = "LIVE" if not dry_run else "DRY RUN"
    logger.info(f"Starting email sending in {mode} mode")
    
    result = send_scheduled_emails(
        scheduled_data=scheduled_data,
        contacts_file=args.contacts,
        dry_run=dry_run,
        start_date=start_date,
        end_date=end_date,
        limit=args.limit,
        delay=args.delay
    )
    
    logger.info(f"Email sending complete: {result['successful']} successful, {result['failed']} failed, {result['total']} total")

if __name__ == "__main__":
    main()