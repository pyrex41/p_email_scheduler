"""
Mock SendGrid client for testing email sending without using the actual SendGrid API.
This module provides a drop-in replacement for the real SendGrid client that logs
email details instead of actually sending emails.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, Union
from datetime import datetime

# Configure logging for the mock
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mock_sendgrid.log', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("mock_sendgrid")

# Directory to store mock emails as JSON files
MOCK_EMAIL_DIR = os.path.join(os.path.dirname(__file__), "mock_emails")
os.makedirs(MOCK_EMAIL_DIR, exist_ok=True)

class MockSendGridClient:
    """Mock client that simulates the SendGridClient but logs emails instead of sending them."""
    
    def __init__(self, api_key: Optional[str] = None, dry_run: Optional[bool] = None):
        """
        Initialize the mock SendGrid client.
        
        Args:
            api_key: Ignored in the mock
            dry_run: Ignored in the mock (always in dry-run mode)
        """
        self.from_email = "mock@example.com"
        self.from_name = "Mock Sender"
        self.dry_run = True  # Always in dry-run mode
        self.client = self  # Self-reference for compatibility
        logger.info(f"Initialized MockSendGridClient with from_email={self.from_email}, from_name={self.from_name}")
    
    def send_email(
        self, 
        to_email: str, 
        subject: str, 
        content: str, 
        html_content: Optional[str] = None,
        dry_run: Optional[bool] = None
    ) -> bool:
        """
        Mock sending an email by logging it and saving to a JSON file.
        
        Args:
            to_email: Recipient email address
            subject: Email subject line
            content: Plain text email content
            html_content: Optional HTML content for the email
            dry_run: Ignored in the mock
            
        Returns:
            Always returns True for successful "sending"
        """
        # Validate email address format (basic check)
        if not to_email or '@' not in to_email:
            logger.error(f"[MOCK] Invalid email address: {to_email}")
            return False
        
        # Create email details dictionary
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        email_details = {
            "timestamp": datetime.now().isoformat(),
            "to_email": to_email,
            "from_email": self.from_email,
            "from_name": self.from_name,
            "subject": subject,
            "content": content[:500] + ("..." if len(content) > 500 else ""),  # Truncate long content
            "html_content": html_content[:500] + ("..." if html_content and len(html_content) > 500 else "") if html_content else None,
            "mock": True
        }
        
        # Log the email
        logger.info(f"[MOCK] Email to: {to_email}, Subject: {subject}")
        logger.info(f"[MOCK] From: {self.from_name} <{self.from_email}>")
        
        # Save to JSON file
        filename = f"mock_email_{timestamp}_{to_email.replace('@', '_at_')}.json"
        filepath = os.path.join(MOCK_EMAIL_DIR, filename)
        
        try:
            with open(filepath, 'w') as f:
                json.dump(email_details, f, indent=2)
            logger.info(f"[MOCK] Email saved to {filepath}")
        except Exception as e:
            logger.error(f"[MOCK] Error saving email to {filepath}: {e}")
        
        return True

# Mock function for the standalone send_email function
def send_email(
    to_email: str, 
    subject: str, 
    content: str, 
    html_content: Optional[str] = None,
    dry_run: Optional[bool] = None
) -> bool:
    """
    Mock of the standalone send_email function.
    
    Args:
        to_email: Recipient email address
        subject: Email subject line
        content: Plain text email content
        html_content: Optional HTML content for the email
        dry_run: Ignored in the mock
        
    Returns:
        Boolean indicating success
    """
    client = MockSendGridClient()
    return client.send_email(to_email, subject, content, html_content, dry_run)

# Function to get all mock emails (for testing/verification)
def get_mock_emails():
    """Return a list of all mock emails that have been 'sent'."""
    emails = []
    for filename in os.listdir(MOCK_EMAIL_DIR):
        if filename.startswith("mock_email_") and filename.endswith(".json"):
            try:
                with open(os.path.join(MOCK_EMAIL_DIR, filename), 'r') as f:
                    emails.append(json.load(f))
            except Exception as e:
                logger.error(f"Error reading mock email file {filename}: {e}")
    
    # Sort by timestamp
    return sorted(emails, key=lambda x: x.get('timestamp', ''))

# Function to clear all mock emails
def clear_mock_emails():
    """Delete all mock email files (for test cleanup)."""
    count = 0
    for filename in os.listdir(MOCK_EMAIL_DIR):
        if filename.startswith("mock_email_") and filename.endswith(".json"):
            try:
                os.remove(os.path.join(MOCK_EMAIL_DIR, filename))
                count += 1
            except Exception as e:
                logger.error(f"Error deleting mock email file {filename}: {e}")
    
    logger.info(f"Cleared {count} mock email files")
    return count