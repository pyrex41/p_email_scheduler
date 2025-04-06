"""
SendGrid Event Webhook Handler

This module handles incoming webhook events from SendGrid to update email delivery statuses.
Events include: delivered, opened, clicked, bounced, dropped, deferred, etc.
"""

import os
import json
import hmac
import base64
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

from email_scheduler_common import logger

class SendGridWebhook:
    """
    Handles incoming SendGrid webhook events for email tracking.
    
    Reference: https://docs.sendgrid.com/for-developers/tracking-events/event
    """
    
    def __init__(self, signing_key: Optional[str] = None):
        """
        Initialize the webhook handler.
        
        Args:
            signing_key: Optional SendGrid webhook signing key for verification
        """
        self.signing_key = signing_key or os.environ.get('SENDGRID_WEBHOOK_KEY', '')
        
    def verify_signature(self, payload: bytes, signature: str, timestamp: str) -> bool:
        """
        Verify the webhook signature to ensure it came from SendGrid.
        
        Args:
            payload: Raw request body bytes
            signature: SendGrid signature from X-Twilio-Email-Event-Webhook-Signature header
            timestamp: Timestamp from X-Twilio-Email-Event-Webhook-Timestamp header
            
        Returns:
            True if signature is valid, False otherwise
        """
        if not self.signing_key:
            logger.warning("No signing key configured for SendGrid webhook verification")
            return True  # Skip verification if no key configured
        
        try:
            # Compute expected signature
            digest = hmac.new(
                key=self.signing_key.encode('utf-8'),
                msg=f"{timestamp}{payload}".encode('utf-8'),
                digestmod=hashlib.sha256
            ).digest()
            expected_signature = base64.b64encode(digest).decode('utf-8')
            
            # Compare signatures
            return hmac.compare_digest(signature, expected_signature)
        except Exception as e:
            logger.error(f"Error verifying SendGrid webhook signature: {e}")
            return False
    
    def process_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process SendGrid webhook events.
        
        Args:
            events: List of event dictionaries from SendGrid
            
        Returns:
            List of processed events with standardized fields
        """
        processed_events = []
        
        for event in events:
            try:
                # Extract standard fields
                message_id = event.get('sg_message_id', '').split('.')[0]
                event_type = event.get('event', 'unknown')
                timestamp = event.get('timestamp', int(datetime.now().timestamp()))
                
                # Convert timestamp to ISO format if it's a unix timestamp
                if isinstance(timestamp, int):
                    timestamp = datetime.fromtimestamp(timestamp).isoformat()
                
                # Process different event types
                processed_event = {
                    'message_id': message_id,
                    'event_type': event_type,
                    'timestamp': timestamp,
                    'email': event.get('email', ''),
                    'raw_event': event
                }
                
                # Add event-specific details
                if event_type == 'delivered':
                    processed_event['response'] = event.get('response', '')
                elif event_type in ['bounce', 'dropped']:
                    processed_event['reason'] = event.get('reason', '')
                    processed_event['status'] = event.get('status', '')
                elif event_type == 'open':
                    processed_event['user_agent'] = event.get('useragent', '')
                    processed_event['ip'] = event.get('ip', '')
                
                processed_events.append(processed_event)
                logger.info(f"Processed SendGrid {event_type} event for message {message_id}")
                
            except Exception as e:
                logger.error(f"Error processing SendGrid event: {e}, event data: {event}")
        
        return processed_events

    def update_email_statuses(self, events: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Update email statuses in the database based on webhook events.
        
        Args:
            events: List of processed events
            
        Returns:
            Dictionary with count of updated statuses by type
        """
        # Import here to avoid circular imports
        from email_status_checker import EmailStatusChecker
        
        # Group events by message ID to find the latest status for each email
        emails_by_message_id = {}
        for event in events:
            message_id = event.get('message_id')
            if not message_id:
                continue
                
            # Store the latest event for each message ID based on timestamp
            if message_id not in emails_by_message_id or \
               event.get('timestamp', '') > emails_by_message_id[message_id].get('timestamp', ''):
                emails_by_message_id[message_id] = event
        
        # Map of SendGrid event types to our internal status values
        status_mapping = {
            'delivered': 'delivered',
            'open': 'delivered',  # If opened, must have been delivered
            'click': 'delivered',  # If clicked, must have been delivered
            'bounce': 'bounced',
            'dropped': 'dropped',
            'deferred': 'deferred',
            'processed': 'sent',
            'sent': 'sent'
        }
        
        # Count of updates by status
        update_counts = {
            'delivered': 0,
            'bounced': 0,
            'dropped': 0,
            'deferred': 0,
            'sent': 0,
            'errors': 0
        }
        
        # Process each unique message ID
        status_checker = EmailStatusChecker()
        for message_id, event in emails_by_message_id.items():
            try:
                # Get the corresponding internal status
                event_type = event.get('event_type', '')
                status = status_mapping.get(event_type, 'unknown')
                
                # Find the email in the database
                org_id, email_id = self._find_email_by_message_id(message_id)
                if not org_id or not email_id:
                    logger.warning(f"Could not find email with message ID {message_id}")
                    continue
                
                # Update the email status in the database
                conn = status_checker.connect_to_org_db(org_id)
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        UPDATE email_send_tracking
                        SET send_status = ?,
                            delivery_status = ?,
                            status_checked_at = ?,
                            status_details = ?
                        WHERE id = ?
                        """,
                        (
                            status,
                            event_type,
                            datetime.now().isoformat(),
                            json.dumps(event.get('raw_event', {})),
                            email_id
                        )
                    )
                    conn.commit()
                    
                    # Update counts
                    if status in update_counts:
                        update_counts[status] += 1
                    
                    logger.info(f"Updated email {email_id} status to {status} based on {event_type} event")
                    
                except Exception as db_err:
                    logger.error(f"Database error updating email {email_id}: {db_err}")
                    update_counts['errors'] += 1
                finally:
                    conn.close()
                    
            except Exception as e:
                logger.error(f"Error updating status for message {message_id}: {e}")
                update_counts['errors'] += 1
        
        return update_counts
    
    def _find_email_by_message_id(self, message_id: str) -> tuple:
        """
        Find the organization ID and email ID for a given message ID.
        
        Args:
            message_id: SendGrid message ID
            
        Returns:
            Tuple of (org_id, email_id) or (None, None) if not found
        """
        # Import here to avoid circular imports
        import os
        import sqlite3
        
        # Get the directory containing organization databases
        base_dir = os.path.dirname(os.path.abspath(__file__))
        org_db_dir = os.path.join(base_dir, "org_dbs")
        
        # Check each organization database
        for filename in os.listdir(org_db_dir):
            if not filename.startswith('org-') or not filename.endswith('.db'):
                continue
                
            try:
                # Extract org ID from filename
                org_id = int(filename.replace('org-', '').replace('.db', ''))
                
                # Connect to the database
                db_path = os.path.join(org_db_dir, filename)
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                
                # Check if the email_send_tracking table exists
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking'")
                if not cursor.fetchone():
                    continue
                
                # Search for the message ID
                cursor.execute(
                    "SELECT id FROM email_send_tracking WHERE message_id = ?",
                    (message_id,)
                )
                result = cursor.fetchone()
                
                if result:
                    email_id = result['id']
                    conn.close()
                    return org_id, email_id
                
                conn.close()
                
            except Exception as e:
                logger.error(f"Error searching for message ID in {filename}: {e}")
        
        return None, None

# Convenience function for handling webhooks
def handle_sendgrid_webhook(
    payload: bytes,
    signature: Optional[str] = None,
    timestamp: Optional[str] = None
) -> Dict[str, Any]:
    """
    Handle an incoming SendGrid webhook request.
    
    Args:
        payload: Raw request body bytes
        signature: Optional SendGrid signature for verification
        timestamp: Optional timestamp for signature verification
        
    Returns:
        Dictionary with processing results
    """
    webhook = SendGridWebhook()
    
    # Verify signature if provided
    if signature and timestamp:
        if not webhook.verify_signature(payload, signature, timestamp):
            logger.warning("Invalid SendGrid webhook signature")
            return {"success": False, "error": "Invalid signature"}
    
    try:
        # Parse events from payload
        events = json.loads(payload)
        if not isinstance(events, list):
            logger.error(f"Expected list of events, got {type(events)}")
            return {"success": False, "error": "Invalid payload format"}
        
        # Process events
        processed_events = webhook.process_events(events)
        
        # Update email statuses
        update_counts = webhook.update_email_statuses(processed_events)
        
        return {
            "success": True,
            "events_processed": len(processed_events),
            "updates": update_counts
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding webhook payload: {e}")
        return {"success": False, "error": "Invalid JSON payload"}
    except Exception as e:
        logger.error(f"Error handling SendGrid webhook: {e}")
        return {"success": False, "error": str(e)}