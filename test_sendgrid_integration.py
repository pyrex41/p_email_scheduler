"""
Test script for verifying SendGrid integration in the email scheduler.
This uses the MockSendGridClient to test the email sending functionality
without making actual API calls to SendGrid.
"""

import os
import sys
import json
import importlib
from unittest import mock
import argparse
from datetime import date, datetime, timedelta
import asyncio

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def patch_sendgrid_imports():
    """
    Patch imports to use the mock SendGrid client instead of the real one.
    
    This function modifies sys.modules to replace the sendgrid_client module
    with our test_sendgrid module, ensuring any imports of SendGridClient
    will use our mock version.
    """
    # Import our mock module
    mock_module = importlib.import_module('test_sendgrid')
    
    # Add it to sys.modules under the name that will be imported
    sys.modules['sendgrid_client'] = mock_module
    
    print("Patched sendgrid_client imports to use mock implementation")
    return mock_module

def run_sendgrid_test(input_file, use_async=True):
    """
    Run the email scheduler with SendGrid email sending enabled.
    
    Args:
        input_file: Path to the input JSON file with contacts
        use_async: Whether to use async processing
        
    Returns:
        Dictionary with test results
    """
    # First patch the imports
    mock_sendgrid = patch_sendgrid_imports()
    
    # Clear any previous mock emails
    mock_sendgrid.clear_mock_emails()
    
    # Import the scheduler (will use our mock SendGrid client)
    from email_scheduler_optimized import main_sync, main_async
    
    # Set up test parameters
    current_date = date.today()
    end_date = current_date + timedelta(days=365)
    
    print(f"Running scheduler with {'async' if use_async else 'sync'} processing")
    print(f"Start date: {current_date}, End date: {end_date}")
    print(f"Using contact data from: {input_file}")
    
    # Load contacts
    with open(input_file, 'r') as f:
        contacts = json.load(f)
        
    # Make sure there are test email addresses
    for i, contact in enumerate(contacts):
        if not contact.get('email'):
            # Add a test email for contacts without one
            contact['email'] = f"test{i}@example.com"
    
    # Run the scheduler with email sending enabled
    if use_async:
        # Use asyncio to run the async version
        print("Running with async processing...")
        results = asyncio.run(main_async(
            contacts,
            current_date,
            end_date,
            batch_size=10,
            max_workers=4,
            send_emails=True  # Enable email sending
        ))
    else:
        # Run the sync version
        print("Running with sync processing...")
        results = main_sync(
            contacts,
            current_date,
            end_date,
            send_emails=True  # Enable email sending
        )
    
    # Get the mock emails that were "sent"
    mock_emails = mock_sendgrid.get_mock_emails()
    
    # Summarize the results
    email_counts_by_type = {}
    for email in mock_emails:
        subject = email.get('subject', '')
        email_type = "unknown"
        
        # Try to extract email type from subject
        if "Birthday" in subject:
            email_type = "birthday"
        elif "Anniversary" in subject:
            email_type = "effective_date"
        elif "Annual Enrollment" in subject:
            email_type = "aep"
        elif "Special Enrollment" in subject:
            email_type = "post_window"
            
        email_counts_by_type[email_type] = email_counts_by_type.get(email_type, 0) + 1
    
    # Print summary
    print("\n===== Test Results =====")
    print(f"Processed {len(contacts)} contacts")
    print(f"Total 'sent' emails: {len(mock_emails)}")
    print("\nEmails by type:")
    for email_type, count in email_counts_by_type.items():
        print(f"  - {email_type}: {count}")
        
    # Return results
    return {
        "contacts_processed": len(contacts),
        "emails_sent": len(mock_emails),
        "emails_by_type": email_counts_by_type,
        "results": results
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test SendGrid integration in email scheduler")
    parser.add_argument("--input", default="./temp_test/contacts.json", help="Input JSON file with contacts")
    parser.add_argument("--sync", action="store_true", help="Use synchronous processing (default is async)")
    
    args = parser.parse_args()
    
    # Set EMAIL_DRY_RUN environment variable for testing
    os.environ["EMAIL_DRY_RUN"] = "true"
    
    # Run the test
    result = run_sendgrid_test(args.input, use_async=not args.sync)
    
    # If we got here, the test was successful
    print("\n✅ SendGrid integration test completed successfully!")