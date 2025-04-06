"""
SendGrid integration module for email scheduler.
Provides functionality to send emails via SendGrid API with support for dry-run mode.
Includes batch sending capabilities for improved performance.
"""

import os
import json
import time
from typing import Dict, Any, Optional, Union, List, Tuple
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content, HtmlContent, Personalization
from email_scheduler_common import logger
from dotenv_config import get_env, get_bool_env, get_email_config

# Default configuration values
DEFAULT_FROM_EMAIL = "medicare@example.com"
DEFAULT_FROM_NAME = "Medicare Services" 
DEFAULT_DRY_RUN = "true"
MAX_BATCH_SIZE = 100  # SendGrid can handle up to 1000, but we'll be more conservative

class SendGridClient:
    """Client for interacting with SendGrid API to send emails, with batch capabilities."""
    
    def __init__(self, api_key: Optional[str] = None, dry_run: Optional[bool] = None):
        """
        Initialize the SendGrid client with API key and settings.
        
        Args:
            api_key: SendGrid API key (if None, reads from SENDGRID_API_KEY env var)
            dry_run: Whether to operate in dry-run mode (if None, reads from EMAIL_DRY_RUN env var)
        """
        # Get email configuration from environment
        email_config = get_email_config()
        
        # Use provided API key or read from environment
        self.api_key = api_key or email_config["api_key"]
        
        # Set up dry run mode (default to True if not specified)
        if dry_run is None:
            self.dry_run = email_config["dry_run"]
        else:
            self.dry_run = dry_run
        
        # Default sender details
        self.from_email = email_config["from_email"]
        self.from_name = email_config["from_name"]
        
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
        Send a single email via SendGrid or log it in dry-run mode.
        
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
            to_email_obj = To(to_email)
            
            # Use HTML content if provided, otherwise use plain text
            if html_content:
                content_obj = HtmlContent(html_content)
            else:
                content_obj = Content("text/plain", content)
            
            # Construct the message
            message = Mail(from_email, to_email_obj, subject, content_obj)
            
            # Send the email
            response = self.client.send(message)
            
            # Check response
            status_code = response.status_code
            
            if 200 <= status_code < 300:  # Success status codes
                logger.info(f"Email sent successfully to {to_email}, status: {status_code}")
                return True
            else:
                # Try to get more details from the response
                try:
                    response_body = response.body.decode('utf-8') if hasattr(response, 'body') and response.body else 'No response body'
                    logger.error(f"Failed to send email to {to_email}, status: {status_code}, details: {response_body}")
                except Exception as decode_err:
                    logger.error(f"Failed to send email to {to_email}, status: {status_code}, could not decode response: {str(decode_err)}")
                
                return False
            
        except Exception as e:
            logger.error(f"Error sending email to {to_email}: {str(e)}")
            return False

    def send_batch(
        self, 
        emails: List[Dict[str, Any]],
        dry_run: Optional[bool] = None
    ) -> Tuple[int, int, List[str]]:
        """
        Send a batch of emails using SendGrid's batch API capability.
        
        Args:
            emails: List of email dictionaries, each containing:
                - to_email: Recipient email address
                - subject: Email subject
                - content: Plain text content
                - html_content: (optional) HTML content
            dry_run: Override instance dry_run setting for this batch
            
        Returns:
            Tuple of (success_count, failed_count, error_messages)
        """
        # Determine dry run mode for this batch
        use_dry_run = self.dry_run if dry_run is None else dry_run
        
        # Initialize counters and error list
        success_count = 0
        failed_count = 0
        errors = []
        
        # Check if batch is empty
        if not emails:
            logger.warning("Empty email batch provided, nothing to send")
            return success_count, failed_count, errors
        
        # Process in sub-batches to stay within API limits
        for i in range(0, len(emails), MAX_BATCH_SIZE):
            sub_batch = emails[i:i+MAX_BATCH_SIZE]
            
            # In dry-run mode, just log the emails
            if use_dry_run:
                for email in sub_batch:
                    logger.info(f"[DRY RUN] Would send email to: {email.get('to_email')}")
                    logger.info(f"[DRY RUN] Subject: {email.get('subject')}")
                    content_preview = email.get('content', '')[:100] + '...' if email.get('content') else 'No content'
                    logger.info(f"[DRY RUN] Content (first 100 chars): {content_preview}")
                
                # All dry-run emails are considered successful
                success_count += len(sub_batch)
                continue
            
            # Ensure we have API key for live mode
            if not self.api_key:
                error_msg = "Cannot send batch: SendGrid API key not provided"
                logger.error(error_msg)
                errors.append(error_msg)
                failed_count += len(sub_batch)
                continue
            
            # Ensure client is initialized
            if not self.client:
                error_msg = "SendGrid client not initialized"
                logger.error(error_msg)
                errors.append(error_msg)
                failed_count += len(sub_batch)
                continue
            
            try:
                # Prepare the batch request using personalizations
                from_email = Email(self.from_email, self.from_name)
                
                # For v3 Mail Send API with personalizations
                mail = Mail()
                mail.from_email = from_email
                
                # Create a separate personalization for each recipient
                for email in sub_batch:
                    to_email = email.get('to_email')
                    subject = email.get('subject')
                    
                    # Skip invalid emails
                    if not to_email or '@' not in to_email:
                        logger.error(f"Invalid email address: {to_email}")
                        failed_count += 1
                        errors.append(f"Invalid email address: {to_email}")
                        continue
                    
                    # Add personalization
                    personalization = Personalization()
                    personalization.add_to(To(to_email))
                    personalization.subject = subject
                    mail.add_personalization(personalization)
                
                # Set content - using the first email's content as default
                if sub_batch and sub_batch[0].get('html_content'):
                    mail.add_content(HtmlContent(sub_batch[0].get('html_content')))
                elif sub_batch and sub_batch[0].get('content'):
                    mail.add_content(Content("text/plain", sub_batch[0].get('content')))
                else:
                    mail.add_content(Content("text/plain", "Email content not provided"))
                
                # Send the batch of emails
                start_time = time.time()
                response = self.client.send(mail)
                end_time = time.time()
                
                # Check response
                status_code = response.status_code
                
                if 200 <= status_code < 300:  # Success status codes
                    batch_size = len(sub_batch)
                    duration = end_time - start_time
                    logger.info(f"Batch of {batch_size} emails sent successfully in {duration:.2f}s, status: {status_code}")
                    success_count += batch_size
                else:
                    batch_size = len(sub_batch)
                    error_msg = f"Failed to send batch of {batch_size} emails, status: {status_code}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    failed_count += batch_size
            
            except Exception as e:
                batch_size = len(sub_batch)
                error_msg = f"Error sending batch of {batch_size} emails: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                failed_count += batch_size
        
        return success_count, failed_count, errors
    
    def send_batch_with_unique_content(
        self, 
        emails: List[Dict[str, Any]],
        dry_run: Optional[bool] = None
    ) -> Tuple[int, int, List[str]]:
        """
        Send a batch of emails where each recipient gets unique content.
        Unlike send_batch which uses personalizations, this sends emails separately but in parallel.
        This is less efficient but allows for completely different content per recipient.
        
        Args:
            emails: List of email dictionaries, each containing:
                - to_email: Recipient email address
                - subject: Email subject
                - content: Plain text content
                - html_content: (optional) HTML content
            dry_run: Override instance dry_run setting for this batch
            
        Returns:
            Tuple of (success_count, failed_count, error_messages)
        """
        # Determine dry run mode for this batch
        use_dry_run = self.dry_run if dry_run is None else dry_run
        
        # Initialize counters and error list
        success_count = 0
        failed_count = 0
        errors = []
        
        # In dry-run mode, just log the emails
        if use_dry_run:
            for email in emails:
                logger.info(f"[DRY RUN] Would send email to: {email.get('to_email')}")
                logger.info(f"[DRY RUN] Subject: {email.get('subject')}")
                content_preview = email.get('content', '')[:100] + '...' if email.get('content') else 'No content'
                logger.info(f"[DRY RUN] Content (first 100 chars): {content_preview}")
            
            # All dry-run emails are considered successful
            return len(emails), 0, []
        
        # Ensure we have API key for live mode
        if not self.api_key:
            error_msg = "Cannot send batch: SendGrid API key not provided"
            logger.error(error_msg)
            return 0, len(emails), [error_msg]
        
        # Ensure client is initialized
        if not self.client:
            error_msg = "SendGrid client not initialized"
            logger.error(error_msg)
            return 0, len(emails), [error_msg]
        
        # Process each email individually (but in a batch request)
        mail_items = []
        
        # Prepare each email
        for email in emails:
            to_email = email.get('to_email')
            subject = email.get('subject')
            
            # Skip invalid emails
            if not to_email or '@' not in to_email:
                logger.error(f"Invalid email address: {to_email}")
                failed_count += 1
                errors.append(f"Invalid email address: {to_email}")
                continue
            
            # Create individual mail
            from_email = Email(self.from_email, self.from_name)
            to_email_obj = To(to_email)
            
            # Prepare email content
            if email.get('html_content'):
                content = HtmlContent(email.get('html_content'))
            elif email.get('content'):
                content = Content("text/plain", email.get('content'))
            else:
                content = Content("text/plain", "Email content not provided")
            
            # Create mail object
            mail = Mail(from_email, to_email_obj, subject, content)
            
            # Prepare for the API
            mail_dict = mail.get()
            mail_items.append(mail_dict)
        
        # Process in sub-batches to stay within API limits
        for i in range(0, len(mail_items), MAX_BATCH_SIZE):
            sub_batch = mail_items[i:i+MAX_BATCH_SIZE]
            
            try:
                start_time = time.time()
                
                # SendGrid v3 API batch send
                response = self.client.client.mail.send.post(request_body=sub_batch)
                
                end_time = time.time()
                
                # Check response
                status_code = response.status_code
                
                if 200 <= status_code < 300:  # Success status codes
                    batch_size = len(sub_batch)
                    duration = end_time - start_time
                    logger.info(f"Batch of {batch_size} unique emails sent successfully in {duration:.2f}s, status: {status_code}")
                    success_count += batch_size
                else:
                    batch_size = len(sub_batch)
                    error_msg = f"Failed to send batch of {batch_size} unique emails, status: {status_code}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    failed_count += batch_size
            
            except Exception as e:
                batch_size = len(sub_batch)
                error_msg = f"Error sending batch of {batch_size} unique emails: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                failed_count += batch_size
        
        return success_count, failed_count, errors

# Convenience functions for standalone use
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

def send_batch(
    emails: List[Dict[str, Any]],
    dry_run: Optional[bool] = None
) -> Tuple[int, int, List[str]]:
    """
    Convenience function to send a batch of emails without managing client instance.
    
    Args:
        emails: List of email dictionaries
        dry_run: Whether to operate in dry-run mode
        
    Returns:
        Tuple of (success_count, failed_count, error_messages)
    """
    client = SendGridClient(dry_run=dry_run)
    return client.send_batch(emails, dry_run)