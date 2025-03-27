"""
SendGrid integration module for email scheduler.
Provides functionality to send emails via SendGrid API with support for dry-run mode.
"""

import os
import logging
from typing import Dict, Any, Optional, Union
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content, HtmlContent
from email_scheduler_common import logger

# Default configuration values
DEFAULT_FROM_EMAIL = "medicare@example.com"
DEFAULT_FROM_NAME = "Medicare Services" 
DEFAULT_DRY_RUN = "true"

class SendGridClient:
    """Client for interacting with SendGrid API to send emails."""
    
    def __init__(self, api_key: Optional[str] = None, dry_run: Optional[bool] = None):
        """
        Initialize the SendGrid client with API key and settings.
        
        Args:
            api_key: SendGrid API key (if None, reads from SENDGRID_API_KEY env var)
            dry_run: Whether to operate in dry-run mode (if None, reads from EMAIL_DRY_RUN env var)
        """
        # Use provided API key or read from environment
        self.api_key = api_key or os.environ.get("SENDGRID_API_KEY")
        
        # Set up dry run mode (default to True if not specified)
        if dry_run is None:
            dry_run_env = os.environ.get("EMAIL_DRY_RUN", DEFAULT_DRY_RUN).lower()
            self.dry_run = dry_run_env in ("true", "1", "yes", "y", "t")
        else:
            self.dry_run = dry_run
        
        # Default sender details
        self.from_email = os.environ.get("FROM_EMAIL", DEFAULT_FROM_EMAIL)
        self.from_name = os.environ.get("FROM_NAME", DEFAULT_FROM_NAME)
        
        # Initialize SendGrid client if API key is available and not in dry-run mode
        self.client = None
        if not self.dry_run and self.api_key:
            try:
                self.client = sendgrid.SendGridAPIClient(api_key=self.api_key)
            except Exception as e:
                logger.error(f"Failed to initialize SendGrid client: {e}")
    
    def send_email(
        self, 
        to_email: str, 
        subject: str, 
        content: str, 
        html_content: Optional[str] = None,
        dry_run: Optional[bool] = None
    ) -> bool:
        """
        Send an email via SendGrid or log it in dry-run mode.
        
        Args:
            to_email: Recipient email address
            subject: Email subject line
            content: Plain text email content
            html_content: Optional HTML content for the email
            dry_run: Override instance dry_run setting for this specific email
            
        Returns:
            Boolean indicating success
        """
        # Determine dry run mode for this specific email
        use_dry_run = self.dry_run if dry_run is None else dry_run
        
        # Validate email address format (basic check)
        if not to_email or '@' not in to_email:
            logger.error(f"Invalid email address: {to_email}")
            return False
        
        # In dry-run mode, just log the email
        if use_dry_run:
            logger.info(f"[DRY RUN] Would send email to: {to_email}")
            logger.info(f"[DRY RUN] Subject: {subject}")
            logger.info(f"[DRY RUN] From: {self.from_name} <{self.from_email}>")
            logger.info(f"[DRY RUN] Content (first 100 chars): {content[:100]}...")
            return True
        
        # Ensure we have API key for live mode
        if not self.api_key:
            logger.error("Cannot send email: SendGrid API key not provided")
            return False
        
        # Ensure client is initialized
        if not self.client:
            logger.error("SendGrid client not initialized")
            return False
        
        try:
            # Create email message
            from_email = Email(self.from_email, self.from_name)
            to_email = To(to_email)
            
            # Use HTML content if provided, otherwise use plain text
            if html_content:
                content = HtmlContent(html_content)
            else:
                content = Content("text/plain", content)
            
            # Construct the message
            message = Mail(from_email, to_email, subject, content)
            
            # Send the email
            response = self.client.send(message)
            
            # Check response
            status_code = response.status_code
            
            if 200 <= status_code < 300:  # Success status codes
                logger.info(f"Email sent successfully to {to_email.email}, status: {status_code}")
                return True
            else:
                logger.error(f"Failed to send email to {to_email.email}, status: {status_code}")
                return False
            
        except Exception as e:
            logger.error(f"Error sending email to {to_email}: {str(e)}")
            return False

# Convenience function to send a single email
def send_email(
    to_email: str, 
    subject: str, 
    content: str, 
    html_content: Optional[str] = None,
    dry_run: Optional[bool] = None
) -> bool:
    """
    Convenience function to send a single email without managing client instance.
    
    Args:
        to_email: Recipient email address
        subject: Email subject line
        content: Plain text email content
        html_content: Optional HTML content for the email
        dry_run: Whether to operate in dry-run mode
        
    Returns:
        Boolean indicating success
    """
    client = SendGridClient(dry_run=dry_run)
    return client.send_email(to_email, subject, content, html_content, dry_run)