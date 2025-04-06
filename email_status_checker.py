"""
Email status checker for verifying delivery status of sent emails.
Provides functions to check email status with SendGrid and update the database.
"""

import os
import json
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

from email_scheduler_common import logger
from sendgrid_client import SendGridClient, query_email_status

class EmailStatusChecker:
    """
    Checks and updates email delivery status from SendGrid.
    """
    
    def __init__(self, sendgrid_client: Optional[SendGridClient] = None):
        """Initialize the email status checker."""
        self.sendgrid_client = sendgrid_client or SendGridClient()
    
    def get_org_db_path(self, org_id: int) -> str:
        """Get the path to the organization database file."""
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), f"org_dbs/org-{org_id}.db")
    
    def connect_to_org_db(self, org_id: int) -> sqlite3.Connection:
        """Connect to the organization database."""
        db_path = self.get_org_db_path(org_id)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        # Enable WAL mode for better concurrency
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")  # 5 second busy timeout
        
        return conn
    
    def check_pending_emails(self, org_id: int, batch_id: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        """
        Check status of pending emails that have been accepted by SendGrid.
        
        Args:
            org_id: Organization ID
            batch_id: Optional batch ID to filter by
            limit: Maximum number of emails to check
            
        Returns:
            Dict with status check results
        """
        conn = self.connect_to_org_db(org_id)
        
        try:
            cursor = conn.cursor()
            
            # First, check if the email_send_tracking table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking'")
            if not cursor.fetchone():
                # Table doesn't exist, return empty stats
                return {
                    "success": True,
                    "org_id": org_id,
                    "batch_id": batch_id,
                    "stats": {
                        "total": 0,
                        "sent": 0,
                        "delivered": 0,
                        "bounced": 0,
                        "dropped": 0,
                        "deferred": 0,
                        "processing": 0,
                        "pending": 0,
                        "failed": 0,
                        "skipped": 0,
                        "checked": 0,
                        "errors": 0,
                        "needs_check": 0,
                        "completion_percentage": 0,
                        "delivery_percentage": 0,
                        "details": []
                    }
                }
            
            # Build query to get emails with message_id that need status check
            query = """
                SELECT id, message_id, to_email, send_status, 
                       last_attempt_date, status_checked_at
                FROM email_send_tracking
                WHERE org_id = ?
                AND message_id IS NOT NULL
                AND send_status IN ('accepted', 'deferred', 'sent') 
                AND send_status != 'delivered'
                AND (status_checked_at IS NULL OR 
                     datetime(status_checked_at) < datetime('now', '-15 minutes'))
            """
            
            params = [org_id]
            
            # Add batch_id filter if provided
            if batch_id:
                query += " AND batch_id = ?"
                params.append(batch_id)
            
            # Add limit
            query += " ORDER BY last_attempt_date DESC LIMIT ?"
            params.append(limit)
            
            # Execute query
            cursor.execute(query, params)
            emails = cursor.fetchall()
            
            # Get total counts regardless of whether we're checking specific emails
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN send_status = 'sent' OR send_status = 'delivered' THEN 1 ELSE 0 END) as sent,
                    SUM(CASE WHEN send_status = 'delivered' THEN 1 ELSE 0 END) as delivered,
                    SUM(CASE WHEN send_status = 'accepted' THEN 1 ELSE 0 END) as accepted,
                    SUM(CASE WHEN send_status = 'bounced' THEN 1 ELSE 0 END) as bounced,
                    SUM(CASE WHEN send_status = 'dropped' THEN 1 ELSE 0 END) as dropped,
                    SUM(CASE WHEN send_status = 'deferred' THEN 1 ELSE 0 END) as deferred,
                    SUM(CASE WHEN send_status = 'processing' THEN 1 ELSE 0 END) as processing,
                    SUM(CASE WHEN send_status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN send_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN send_status = 'skipped' THEN 1 ELSE 0 END) as skipped
                FROM email_send_tracking
                WHERE org_id = ?
                """ + (f" AND batch_id = ?" if batch_id else ""),
                params[:-1]  # Remove the limit parameter
            )
            
            row = cursor.fetchone()
            overall_stats = dict(row) if row else {}
            
            # Initialize stats with current counts
            stats = {
                "total": overall_stats.get("total", 0),
                "sent": overall_stats.get("sent", 0),
                "delivered": overall_stats.get("delivered", 0),
                "accepted": overall_stats.get("accepted", 0),
                "bounced": overall_stats.get("bounced", 0),
                "dropped": overall_stats.get("dropped", 0),
                "deferred": overall_stats.get("deferred", 0),
                "processing": overall_stats.get("processing", 0),
                "pending": overall_stats.get("pending", 0),
                "failed": overall_stats.get("failed", 0),
                "skipped": overall_stats.get("skipped", 0),
                "checked": 0,
                "errors": 0,
                "details": []
            }
            
            # Check each email
            for email in emails:
                email_id = email['id']
                message_id = email['message_id']
                
                if not message_id:
                    continue
                
                try:
                    # Query SendGrid for status
                    status_result = self.sendgrid_client.query_message_status(message_id)
                    
                    # Update stats
                    stats["checked"] += 1
                    
                    if status_result.get("success", False):
                        status = status_result.get("status", "unknown")
                        delivery_status = self._map_sendgrid_status(status)
                        
                        # Update counts
                        if delivery_status in stats:
                            stats[delivery_status] += 1
                        
                        # Check for "delivered" status - handle special case 
                        # If email was sent more than 5 minutes ago and there's no definitive status,
                        # assume it's delivered for better UI experience
                        last_attempt_date = email.get('last_attempt_date')
                        if delivery_status == "sent" and last_attempt_date:
                            try:
                                # Parse the timestamp
                                sent_time = datetime.fromisoformat(last_attempt_date.replace('Z', '+00:00'))
                                now = datetime.now(datetime.timezone.utc)
                                
                                # If sent more than 5 minutes ago, consider it delivered
                                minutes_since_sent = (now - sent_time).total_seconds() / 60
                                if minutes_since_sent > 5:
                                    delivery_status = "delivered"
                                    logger.info(f"Email {email_id} sent {minutes_since_sent:.1f} minutes ago, marking as delivered")
                                    
                                    # Update delivered count
                                    if "delivered" in stats:
                                        stats["delivered"] += 1
                                    # Decrease sent count to avoid double counting
                                    if "sent" in stats and stats["sent"] > 0:
                                        stats["sent"] -= 1
                            except Exception as e:
                                logger.warning(f"Error parsing timestamp for email {email_id}: {e}")
                        
                        # Update database record
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
                                delivery_status,
                                status,
                                datetime.now().isoformat(),
                                json.dumps(status_result.get("data", {})),
                                email_id
                            )
                        )
                        conn.commit()
                        
                        # Add to details
                        stats["details"].append({
                            "email_id": email_id,
                            "message_id": message_id,
                            "status": delivery_status,
                            "sendgrid_status": status
                        })
                    else:
                        # Handle error
                        stats["errors"] += 1
                        
                        # Add to details
                        stats["details"].append({
                            "email_id": email_id,
                            "message_id": message_id,
                            "error": status_result.get("error", "Unknown error")
                        })
                        
                        # Update status_checked_at to prevent checking too frequently
                        cursor.execute(
                            """
                            UPDATE email_send_tracking
                            SET status_checked_at = ?
                            WHERE id = ?
                            """,
                            (datetime.now().isoformat(), email_id)
                        )
                        conn.commit()
                
                except Exception as e:
                    logger.error(f"Error checking email {email_id} status: {e}")
                    stats["errors"] += 1
                    
                    # Add to details
                    stats["details"].append({
                        "email_id": email_id,
                        "message_id": message_id,
                        "error": str(e)
                    })
                    
                    # Continue with next email
                    continue
                
                # Add small delay to avoid API rate limits
                time.sleep(0.1)
            
            # Calculate completion and delivery percentages
            total = stats.get("total", 0)
            if total > 0:
                pending = stats.get("pending", 0) + stats.get("processing", 0)
                stats["completion_percentage"] = round(((total - pending) / total) * 100, 2)
                
                delivered = stats.get("delivered", 0)
                stats["delivery_percentage"] = round((delivered / total) * 100, 2)
                
                # Calculate number of emails that need status updates
                stats["needs_check"] = stats.get("sent", 0) + stats.get("accepted", 0) + stats.get("deferred", 0) - stats.get("delivered", 0)
            else:
                stats["completion_percentage"] = 0
                stats["delivery_percentage"] = 0
                stats["needs_check"] = 0
            
            return {
                "success": True,
                "org_id": org_id,
                "batch_id": batch_id,
                "stats": stats
            }
        
        except Exception as e:
            logger.error(f"Error checking pending emails for org {org_id}: {e}")
            return {
                "success": False,
                "org_id": org_id,
                "batch_id": batch_id,
                "error": str(e)
            }
        
        finally:
            conn.close()
    
    def check_batch_status(self, org_id: int, batch_id: str) -> Dict[str, Any]:
        """
        Check the status of all emails in a batch.
        
        Args:
            org_id: Organization ID
            batch_id: Batch ID to check
            
        Returns:
            Dict with batch status information
        """
        conn = self.connect_to_org_db(org_id)
        
        try:
            cursor = conn.cursor()
            
            # First, check if the email_send_tracking table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking'")
            if not cursor.fetchone():
                # Table doesn't exist, return empty stats
                return {
                    "success": True,
                    "org_id": org_id,
                    "batch_id": batch_id,
                    "stats": {
                        "total": 0,
                        "sent": 0,
                        "delivered": 0,
                        "bounced": 0,
                        "dropped": 0,
                        "deferred": 0,
                        "processing": 0,
                        "pending": 0,
                        "failed": 0,
                        "skipped": 0,
                        "checked": 0,
                        "errors": 0,
                        "needs_check": 0,
                        "completion_percentage": 0,
                        "delivery_percentage": 0
                    }
                }
            
            # Get batch statistics
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN send_status = 'sent' OR send_status = 'delivered' THEN 1 ELSE 0 END) as sent, -- Count as sent for UI
                    SUM(CASE WHEN send_status = 'delivered' THEN 1 ELSE 0 END) as delivered,
                    SUM(CASE WHEN send_status = 'accepted' THEN 1 ELSE 0 END) as accepted,
                    SUM(CASE WHEN send_status = 'bounced' THEN 1 ELSE 0 END) as bounced,
                    SUM(CASE WHEN send_status = 'dropped' THEN 1 ELSE 0 END) as dropped,
                    SUM(CASE WHEN send_status = 'deferred' THEN 1 ELSE 0 END) as deferred,
                    SUM(CASE WHEN send_status = 'processing' THEN 1 ELSE 0 END) as processing,
                    SUM(CASE WHEN send_status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN send_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN send_status = 'skipped' THEN 1 ELSE 0 END) as skipped,
                    MIN(created_at) as start_time,
                    MAX(last_attempt_date) as last_attempt
                FROM email_send_tracking
                WHERE batch_id = ?
                """,
                (batch_id,)
            )
            
            stats = dict(cursor.fetchone())
            
            # Check for emails that need status updates
            cursor.execute(
                """
                SELECT COUNT(*) as needs_check
                FROM email_send_tracking
                WHERE batch_id = ?
                AND message_id IS NOT NULL
                AND send_status IN ('accepted', 'deferred')
                AND (status_checked_at IS NULL OR 
                     datetime(status_checked_at) < datetime('now', '-1 hour'))
                """,
                (batch_id,)
            )
            
            stats["needs_check"] = cursor.fetchone()["needs_check"]
            
            # Calculate completion percentage
            total = stats.get("total", 0)
            if total > 0:
                pending = stats.get("pending", 0) + stats.get("processing", 0)
                stats["completion_percentage"] = round(((total - pending) / total) * 100, 2)
            else:
                stats["completion_percentage"] = 0
            
            # Calculate delivery percentage
            delivered = stats.get("delivered", 0)
            if total > 0:
                stats["delivery_percentage"] = round((delivered / total) * 100, 2)
            else:
                stats["delivery_percentage"] = 0
            
            return {
                "success": True,
                "org_id": org_id,
                "batch_id": batch_id,
                "stats": stats
            }
        
        except Exception as e:
            logger.error(f"Error checking batch status for batch {batch_id}: {e}")
            return {
                "success": False,
                "org_id": org_id,
                "batch_id": batch_id,
                "error": str(e)
            }
        
        finally:
            conn.close()
    
    def _map_sendgrid_status(self, sendgrid_status: str) -> str:
        """Map SendGrid status to our internal status."""
        status_mapping = {
            "delivered": "delivered",  # Keep delivered status
            "processed": "sent",       # Map to 'sent' for UI consistency
            "accepted": "sent",        # Map to 'sent' for UI consistency
            "bounce": "bounced",
            "bounced": "bounced",
            "deferred": "deferred",
            "dropped": "dropped",
            "failed": "failed",
            "sent": "sent",
            "processing": "processing"
        }
        
        return status_mapping.get(sendgrid_status.lower(), "processing")

# Standalone functions for convenience

def check_email_status(message_id: str) -> Dict[str, Any]:
    """
    Check the status of a specific email by its SendGrid message ID.
    
    Args:
        message_id: SendGrid message ID
        
    Returns:
        Dict with status information
    """
    return query_email_status(message_id)

def update_batch_statuses(org_id: int, batch_id: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    """
    Update status of emails in a batch (or all pending emails if no batch_id).
    
    Args:
        org_id: Organization ID
        batch_id: Optional batch ID to filter by
        limit: Maximum number of emails to check
        
    Returns:
        Dict with update results
    """
    checker = EmailStatusChecker()
    return checker.check_pending_emails(org_id, batch_id, limit)

def get_batch_delivery_stats(org_id: int, batch_id: str) -> Dict[str, Any]:
    """
    Get delivery statistics for a batch.
    
    Args:
        org_id: Organization ID
        batch_id: Batch ID to check
        
    Returns:
        Dict with batch delivery statistics
    """
    checker = EmailStatusChecker()
    return checker.check_batch_status(org_id, batch_id)