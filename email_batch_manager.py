"""
Email Batch Manager for managing email batches.
Implements batch processing for sending emails with tracking.
"""

import json
import os
import sqlite3
import aiosqlite
import uuid
import asyncio
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

from email_scheduler_common import logger
from sendgrid_client import SendGridClient
from email_template_engine import EmailTemplateEngine

# Initialize email template engine
template_engine = EmailTemplateEngine()

class EmailBatchManager:
    """
    Manages email batches and sending process with tracking.
    """
    
    def __init__(self):
        """Initialize the email batch manager."""
        # Initialize the SendGrid client with dry_run=False so we can control
        # whether to send real emails based on the send_mode in the database
        from dotenv_config import load_env
        # Ensure environment variables are loaded
        load_env()
        self.sendgrid_client = SendGridClient(dry_run=False)
    
    def get_org_db_path(self, org_id: int) -> str:
        """Get the path to the organization database file."""
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), f"org_dbs/org-{org_id}.db")
    
    def connect_to_org_db(self, org_id: int) -> sqlite3.Connection:
        """Connect to the organization database."""
        db_path = self.get_org_db_path(org_id)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    async def connect_to_org_db_async(self, org_id: int) -> aiosqlite.Connection:
        """Connect to the organization database asynchronously."""
        db_path = self.get_org_db_path(org_id)
        conn = await aiosqlite.connect(db_path)
        # Set row factory to return dictionaries
        conn.row_factory = lambda cursor, row: {
            col[0]: row[idx] for idx, col in enumerate(cursor.description)
        }
        return conn
    
    def initialize_batch_single_email(
        self,
        org_id: int,
        contact_ids: List[str],
        email_type: str,
        send_mode: str,
        test_email: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Initialize a new email sending batch with exactly ONE email per contact.
        Used for single type manual batches where we don't want multiple emails per contact.
        
        Args:
            org_id: Organization ID
            contact_ids: List of contact IDs to include
            email_type: Single email type to use
            send_mode: 'test' or 'production'
            test_email: Email address for test mode
            
        Returns:
            Dict with batch_id and total_emails
        """
        # Validate send mode
        if send_mode not in ['test', 'production']:
            raise ValueError(f"Invalid send mode: {send_mode}")
        
        # For test mode, test_email is required
        if send_mode == 'test' and not test_email:
            raise ValueError("Test email is required for test mode")
            
        # Validate email type
        valid_email_types = ['birthday', 'effective_date', 'anniversary', 'aep', 'post_window']
        if email_type not in valid_email_types:
            raise ValueError(f"Invalid email type: {email_type}")
        
        # Generate batch ID with explicit "single" indicator
        batch_id = f"batch_single_{uuid.uuid4().hex[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        today = date.today()
        today_str = today.isoformat()
        
        # Connect to database
        conn = self.connect_to_org_db(org_id)
        
        try:
            # Create the email_send_tracking table if it doesn't exist
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations/add_email_tracking.sql"), 'r') as f:
                migration_sql = f.read()
                conn.executescript(migration_sql)
            
            # Start a transaction
            cursor = conn.cursor()
            
            # Get a set of unique contact IDs
            unique_contact_ids = set(contact_ids)
            total_emails = 0
            
            # Insert one email per contact
            for contact_id in unique_contact_ids:
                cursor.execute(
                    """
                    INSERT INTO email_send_tracking 
                    (org_id, contact_id, email_type, scheduled_date, send_status, send_mode, test_email, batch_id)
                    VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        org_id, 
                        contact_id, 
                        email_type, 
                        today_str, 
                        send_mode, 
                        test_email if send_mode == 'test' else None, 
                        batch_id
                    )
                )
                total_emails += 1
            
            # Commit the transaction
            conn.commit()
            
            logger.info(f"Initialized single-email batch with {total_emails} unique contacts")
            
            return {
                "batch_id": batch_id,
                "total_emails": total_emails,
                "org_id": org_id,
                "mode": "single_email"
            }
        
        except Exception as e:
            # Rollback on error
            conn.rollback()
            logger.error(f"Error initializing single-email batch: {e}")
            raise
        finally:
            conn.close()
    
    def initialize_batch(
        self,
        org_id: int,
        contact_ids: List[str],
        email_types: List[str],
        send_mode: str,
        test_email: Optional[str] = None,
        scope: str = 'all'
    ) -> str:
        """
        Initialize a new email sending batch.
        
        Args:
            org_id: Organization ID
            contact_ids: List of contact IDs to include
            email_types: List of email types to include
            send_mode: 'test' or 'production'
            test_email: Email address for test mode
            scope: 'today', 'next_7_days', 'next_30_days', or 'all'
            
        Returns:
            batch_id: The generated batch ID string
        """
        start_time = time.time()
        logger.info(f"Initializing batch for org_id={org_id}, {len(contact_ids)} contacts, types={email_types}")
        
        # Validate send mode
        if send_mode not in ['test', 'production']:
            raise ValueError(f"Invalid send mode: {send_mode}")
        
        # For test mode, test_email is required
        if send_mode == 'test' and not test_email:
            raise ValueError("Test email is required for test mode")
            
        # Validate email types
        valid_email_types = ['birthday', 'effective_date', 'anniversary', 'aep', 'post_window']
        for email_type in email_types:
            if email_type not in valid_email_types:
                raise ValueError(f"Invalid email type: {email_type}")
        
        # Generate batch ID with timestamp for better tracking
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        batch_id = f"batch_{uuid.uuid4().hex[:10]}_{timestamp}"
        logger.info(f"Generated batch ID: {batch_id}")
        
        # Calculate date range based on scope
        today = date.today()
        date_ranges = {
            'today': (today, today),
            'next_7_days': (today, today + timedelta(days=7)),
            'next_30_days': (today, today + timedelta(days=30)),
            'all': (today, today + timedelta(days=365)),  # Default to 1 year
            'bulk': (date(2000, 1, 1), date(2100, 1, 1))  # Very wide range for bulk mode
        }
        
        start_date, end_date = date_ranges.get(scope, date_ranges['all'])
        
        # Load scheduled emails from JSON files for each contact
        # This is a simplified approach - in a real implementation,
        # you would load the scheduled emails from your database or API
        schedule_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_dir")
        schedule_file = os.path.join(schedule_directory, "scheduled_emails.json")
        
        try:
            with open(schedule_file, 'r') as f:
                scheduled_data = json.load(f)
        except Exception as e:
            logger.error(f"Error loading scheduled emails: {e}")
            scheduled_data = []
        
        # Filter scheduled emails based on contact IDs, email types, and date range
        total_emails = 0
        conn = self.connect_to_org_db(org_id)
        
        try:
            # Create the email_send_tracking table if it doesn't exist
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations/add_email_tracking.sql"), 'r') as f:
                migration_sql = f.read()
                conn.executescript(migration_sql)
                
            # Start a transaction
            cursor = conn.cursor()
            
            # Special handling for bulk mode - create an email for each contact
            if scope == 'bulk':
                # Get all contact IDs we should include
                included_contact_ids = set()
                
                if contact_ids:
                    # If specific contacts were provided, use those
                    included_contact_ids = set(contact_ids)
                else:
                    # Otherwise, use all contacts from scheduled_data
                    for contact_data in scheduled_data:
                        included_contact_ids.add(str(contact_data.get('contact_id')))
                
                # For each contact, create one email per selected email type, all with today's date
                today_str = today.isoformat()
                
                for contact_id in included_contact_ids:
                    # Create one record for each selected email type
                    for email_type in email_types:
                        cursor.execute(
                            """
                            INSERT INTO email_send_tracking 
                            (org_id, contact_id, email_type, scheduled_date, send_status, send_mode, test_email, batch_id)
                            VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
                            """,
                            (
                                org_id, 
                                contact_id, 
                                email_type, 
                                today_str, 
                                send_mode, 
                                test_email if send_mode == 'test' else None, 
                                batch_id
                            )
                        )
                        total_emails += 1
            
            # Regular mode - process each contact's scheduled emails
            else:
                for contact_data in scheduled_data:
                    contact_id = contact_data.get('contact_id')
                    
                    # Skip if contact_id not in the list
                    if contact_ids and str(contact_id) not in contact_ids:
                        continue
                    
                    scheduled_emails = contact_data.get('emails', [])
                    
                    for email in scheduled_emails:
                        email_type = email.get('type')
                        email_date_str = email.get('date')
                        
                        # Skip if email type not in the list or the email is marked as skipped
                        if email_type not in email_types or email.get('skipped', False):
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
                        
                        # Create a record in the email_send_tracking table
                        cursor.execute(
                            """
                            INSERT INTO email_send_tracking 
                            (org_id, contact_id, email_type, scheduled_date, send_status, send_mode, test_email, batch_id)
                            VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
                            """,
                            (
                                org_id, 
                                contact_id, 
                                email_type, 
                                email_date_str, 
                                send_mode, 
                                test_email if send_mode == 'test' else None, 
                                batch_id
                            )
                        )
                        
                        total_emails += 1
            
            # Commit the transaction
            conn.commit()
            
            # Get the final count
            cursor.execute(
                "SELECT COUNT(*) FROM email_send_tracking WHERE batch_id = ?", 
                (batch_id,)
            )
            total_emails = cursor.fetchone()[0]
            
            # Calculate duration for performance monitoring
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"Batch {batch_id} initialized with {total_emails} emails in {duration:.2f}s")
            
            # Return just the batch ID as per our updated return type
            return batch_id
        
        except Exception as e:
            # Rollback on error
            conn.rollback()
            logger.error(f"Error initializing batch: {e}")
            raise
        finally:
            conn.close()
    
    def process_batch_chunk(
        self, 
        batch_id: str, 
        chunk_size: int = 25
    ) -> Dict[str, Any]:
        """
        Process a chunk of emails from a batch.
        
        Args:
            batch_id: The batch ID to process
            chunk_size: Number of emails to process in this chunk
            
        Returns:
            Dict with processing results
        """
        if chunk_size < 1 or chunk_size > 100:
            raise ValueError("Chunk size must be between 1 and 100")
        
        # Find the organization ID for this batch
        org_id = self._get_org_id_for_batch(batch_id)
        if not org_id:
            raise ValueError(f"No batch found with ID: {batch_id}")
        
        conn = self.connect_to_org_db(org_id)
        cursor = conn.cursor()
        
        try:
            # Get pending emails for this batch
            cursor.execute(
                """
                SELECT id, org_id, contact_id, email_type, scheduled_date, send_mode, test_email
                FROM email_send_tracking
                WHERE batch_id = ? AND send_status = 'pending'
                ORDER BY scheduled_date
                LIMIT ?
                """,
                (batch_id, chunk_size)
            )
            
            pending_emails = cursor.fetchall()
            
            # Process results
            processed_count = len(pending_emails)
            successful_count = 0
            failed_count = 0
            errors = []
            
            # Process each email
            for email in pending_emails:
                email_id = email['id']
                contact_id = email['contact_id']
                email_type = email['email_type']
                scheduled_date = email['scheduled_date']
                send_mode = email['send_mode']
                test_email = email['test_email']
                
                try:
                    # Get contact details
                    contact = self._get_contact_details(org_id, contact_id)
                    if not contact:
                        raise ValueError(f"Contact {contact_id} not found")
                    
                    # Get email content
                    try:
                        email_date = datetime.strptime(scheduled_date, "%Y-%m-%d").date()
                        content = template_engine.render_email(email_type, contact, email_date)
                        html_content = template_engine.render_email(email_type, contact, email_date, html=True)
                    except Exception as render_error:
                        logger.error(f"Error rendering email: {render_error}")
                        raise ValueError(f"Failed to render {email_type} email: {str(render_error)[:100]}")
                    
                    # Determine recipient email
                    to_email = test_email if send_mode == 'test' else contact.get('email')
                    if not to_email:
                        raise ValueError(f"No recipient email address available")
                    
                    # Modify subject for test mode
                    subject = content['subject']
                    if send_mode == 'test':
                        subject = f"[TEST] {subject}"
                    
                    # Check if we're allowed to send emails in this mode according to app settings
                    can_send = self._can_send_in_mode(send_mode)
                    
                    # Both test and production modes send actual emails if enabled
                    # The only difference is the recipient and subject prefix
                    result = self.sendgrid_client.send_email(
                        to_email=to_email,
                        subject=subject,
                        content=content['body'],
                        html_content=html_content['html'],  # Extract just the HTML string from the dictionary
                        # Use dry_run if sending is disabled for this mode
                        dry_run=not can_send
                    )
                    
                    if result:
                        # Update record as sent
                        cursor.execute(
                            """
                            UPDATE email_send_tracking
                            SET send_status = 'sent', 
                                send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?
                            WHERE id = ?
                            """,
                            (datetime.now().isoformat(), email_id)
                        )
                        successful_count += 1
                    else:
                        # Update record as failed
                        cursor.execute(
                            """
                            UPDATE email_send_tracking
                            SET send_status = 'failed', 
                                send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?,
                                last_error = ?
                            WHERE id = ?
                            """,
                            (datetime.now().isoformat(), "Failed to send email", email_id)
                        )
                        failed_count += 1
                        errors.append(f"Failed to send {email_type} email to contact {contact_id}")
                
                except Exception as e:
                    error_message = str(e)
                    # Update record as failed
                    cursor.execute(
                        """
                        UPDATE email_send_tracking
                        SET send_status = 'failed', 
                            send_attempt_count = send_attempt_count + 1,
                            last_attempt_date = ?,
                            last_error = ?
                        WHERE id = ?
                        """,
                        (datetime.now().isoformat(), error_message[:500], email_id)
                    )
                    failed_count += 1
                    errors.append(f"Error sending {email_type} email to contact {contact_id}: {error_message[:100]}")
                    logger.error(f"Error sending email {email_id}: {e}")
            
            # Commit changes
            conn.commit()
            
            # Get remaining count
            cursor.execute(
                "SELECT COUNT(*) FROM email_send_tracking WHERE batch_id = ? AND send_status = 'pending'",
                (batch_id,)
            )
            remaining_count = cursor.fetchone()[0]
            
            return {
                "processed": processed_count,
                "sent": successful_count,
                "failed": failed_count,
                "remaining": remaining_count,
                "errors": errors
            }
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing batch chunk: {e}")
            raise
        finally:
            conn.close()
    
    
    async def process_batch_chunk_async(
        self, 
        batch_id: str, 
        chunk_size: int = 25,
        delay: float = 0
    ) -> Dict[str, Any]:
        """
        Process a chunk of emails from a batch asynchronously using high-performance parallel processing.
        
        Args:
            batch_id: The batch ID to process
            chunk_size: Number of emails to process in this chunk
            delay: Optional delay between sending emails (in seconds)
            
        Returns:
            Dict with processing results
        """
        if chunk_size < 1 or chunk_size > 100:
            raise ValueError("Chunk size must be between 1 and 100")
        
        # Limit concurrent operations to avoid overwhelming the system
        # This creates a semaphore that allows up to 10 tasks to run concurrently
        semaphore = asyncio.Semaphore(10)
        
        # Find the organization ID for this batch
        org_id = self._get_org_id_for_batch(batch_id)
        if not org_id:
            raise ValueError(f"No batch found with ID: {batch_id}")
            
        start_time = time.time()
        logger.info(f"Starting batch processing for batch {batch_id}, chunk size {chunk_size}")
        
        # Create a connection pool for database operations
        conn = await self.connect_to_org_db_async(org_id)
        
        try:
            # Get pending emails for this batch
            cursor = await conn.execute(
                """
                SELECT id, org_id, contact_id, email_type, scheduled_date, send_mode, test_email
                FROM email_send_tracking
                WHERE batch_id = ? AND send_status = 'pending'
                ORDER BY scheduled_date
                LIMIT ?
                """,
                (batch_id, chunk_size)
            )
            
            pending_emails = await cursor.fetchall()
            
            # Process results
            processed_count = len(pending_emails)
            
            if processed_count == 0:
                logger.info(f"No pending emails found for batch {batch_id}")
                return {
                    "processed": 0,
                    "sent": 0,
                    "failed": 0,
                    "remaining": 0,
                    "errors": [],
                    "duration_seconds": 0
                }
            
            logger.info(f"Processing {processed_count} emails for batch {batch_id}")
            
            # Prefetch all send_mode permissions to avoid repeated lookups
            # Cache the results for fast lookup during email processing
            send_modes = set(email['send_mode'] for email in pending_emails)
            send_mode_permissions = {
                mode: await asyncio.to_thread(self._can_send_in_mode, mode)
                for mode in send_modes
            }
            
            # Create a function to process a single email asynchronously
            async def process_single_email(email_record):
                async with semaphore:  # Limit concurrent operations
                    try:
                        email_id = email_record['id']
                        contact_id = email_record['contact_id']
                        email_type = email_record['email_type']
                        scheduled_date = email_record['scheduled_date']
                        send_mode = email_record['send_mode']
                        test_email = email_record['test_email']
                        
                        # Get contact details
                        contact = await asyncio.to_thread(self._get_contact_details, org_id, contact_id)
                        if not contact:
                            logger.error(f"Contact {contact_id} not found, skipping email {email_id}")
                            return {
                                'status': 'failed',
                                'email_id': email_id,
                                'error': f"Contact {contact_id} not found"
                            }
                        
                        # Parse the email date
                        email_date = datetime.strptime(scheduled_date, "%Y-%m-%d").date()
                        
                        # Render email content in parallel using asyncio.gather
                        content_task = asyncio.create_task(
                            asyncio.to_thread(template_engine.render_email, email_type, contact, email_date)
                        )
                        html_content_task = asyncio.create_task(
                            asyncio.to_thread(template_engine.render_email, email_type, contact, email_date, html=True)
                        )
                        
                        # Wait for both render tasks to complete
                        content, html_content = await asyncio.gather(content_task, html_content_task)
                        
                        # Determine recipient email
                        to_email = test_email if send_mode == 'test' else contact.get('email')
                        if not to_email:
                            logger.error(f"No email address for contact {contact_id}, email {email_id}")
                            return {
                                'status': 'failed',
                                'email_id': email_id,
                                'error': f"No email address for contact {contact_id}"
                            }
                        
                        # Modify subject for test mode
                        subject = content['subject']
                        if send_mode == 'test':
                            subject = f"[TEST] {subject}"
                        
                        # Check if we can send emails in this mode
                        allow_send = send_mode_permissions.get(send_mode, False)
                        
                        # Prepare email data
                        email_data = {
                            'to_email': to_email,
                            'subject': subject,
                            'content': content['body'],
                            'html_content': html_content,
                            'email_id': email_id,
                            'contact_id': contact_id,
                            'email_type': email_type
                        }
                        
                        # Send the email (wrapped in to_thread to make the synchronous call non-blocking)
                        success = await asyncio.to_thread(
                            self.sendgrid_client.send_email,
                            to_email=to_email,
                            subject=subject,
                            content=content['body'],
                            html_content=html_content['html'],  # Extract just the HTML string from the dictionary
                            dry_run=not allow_send
                        )
                        
                        if success:
                            return {
                                'status': 'sent',
                                'email_id': email_id
                            }
                        else:
                            return {
                                'status': 'failed',
                                'email_id': email_id,
                                'error': f"Failed to send email to {to_email}"
                            }
                    except Exception as e:
                        error_message = str(e)
                        logger.error(f"Error processing email {email_record['id']}: {error_message}")
                        return {
                            'status': 'failed',
                            'email_id': email_record['id'],
                            'error': error_message[:500]
                        }
            
            # Process all emails in parallel using asyncio.gather
            # This launches all email processing tasks at once and waits for them to complete
            results = await asyncio.gather(
                *[process_single_email(email) for email in pending_emails]
            )
            
            # Collect results
            successful_ids = [r['email_id'] for r in results if r['status'] == 'sent']
            failed_results = [r for r in results if r['status'] == 'failed']
            failed_ids = [r['email_id'] for r in failed_results]
            errors = [r.get('error', 'Unknown error') for r in failed_results]
            
            # Optimize database updates by doing them in batches
            # First update all successful emails at once
            if successful_ids:
                placeholders = ",".join(["?" for _ in successful_ids])
                timestamp = datetime.now().isoformat()
                await conn.execute(
                    f"""
                    UPDATE email_send_tracking
                    SET send_status = 'sent', 
                        send_attempt_count = send_attempt_count + 1,
                        last_attempt_date = ?
                    WHERE id IN ({placeholders})
                    """,
                    [timestamp] + successful_ids
                )
            
            # Then update all failed emails at once (with individual error messages)
            if failed_ids:
                # Prepare batch update for failed emails
                update_tasks = []
                timestamp = datetime.now().isoformat()
                
                for failed_result in failed_results:
                    email_id = failed_result['email_id']
                    error_msg = failed_result.get('error', 'Unknown error')[:500]
                    
                    update_tasks.append(
                        conn.execute(
                            """
                            UPDATE email_send_tracking
                            SET send_status = 'failed', 
                                send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?,
                                last_error = ?
                            WHERE id = ?
                            """,
                            (timestamp, error_msg, email_id)
                        )
                    )
                
                # Execute all updates in parallel
                await asyncio.gather(*update_tasks)
            
            # Commit changes
            await conn.commit()
            
            # Get remaining count
            cursor = await conn.execute(
                "SELECT COUNT(*) as count FROM email_send_tracking WHERE batch_id = ? AND send_status = 'pending'",
                (batch_id,)
            )
            result = await cursor.fetchone()
            remaining_count = result['count']
            
            # Calculate duration
            end_time = time.time()
            duration = end_time - start_time
            
            # Log performance metrics
            success_count = len(successful_ids)
            failed_count = len(failed_ids)
            emails_per_second = processed_count / duration if duration > 0 else 0
            
            logger.info(
                f"Batch {batch_id} processed in {duration:.2f}s: " +
                f"{success_count} sent, {failed_count} failed, " +
                f"{emails_per_second:.1f} emails/second, {remaining_count} remaining"
            )
            
            # Return detailed results
            return {
                "processed": processed_count,
                "sent": success_count,
                "failed": failed_count,
                "remaining": remaining_count,
                "errors": errors[:10],  # Limit to first 10 to avoid overflow
                "duration_seconds": duration,
                "emails_per_second": emails_per_second
            }
        
        except Exception as e:
            await conn.rollback()
            logger.error(f"Error processing batch chunk: {e}")
            raise
        finally:
            await conn.close()
    
    def resume_batch(self, batch_id: str, chunk_size: int = 100) -> Dict[str, Any]:
        """
        Resume processing a batch by processing the next chunk of pending emails.
        
        Args:
            batch_id: The batch ID to resume
            chunk_size: Number of emails to process in this chunk
            
        Returns:
            Dict with processing results (same as process_batch_chunk)
        """
        # Simply call process_batch_chunk with the provided parameters
        return self.process_batch_chunk(batch_id, chunk_size)
    
    async def resume_batch_async(self, batch_id: str, chunk_size: int = 100, delay: float = 0) -> Dict[str, Any]:
        """
        Resume processing a batch by processing the next chunk of pending emails asynchronously.
        
        Args:
            batch_id: The batch ID to resume
            chunk_size: Number of emails to process in this chunk
            delay: Optional delay between sending emails (in seconds)
            
        Returns:
            Dict with processing results (same as process_batch_chunk_async)
        """
        # Simply call process_batch_chunk_async with the provided parameters
        return await self.process_batch_chunk_async(batch_id, chunk_size, delay)
    
    def retry_failed_emails(self, batch_id: str, chunk_size: int = 100) -> Dict[str, Any]:
        """
        Retry failed emails for a batch.
        
        Args:
            batch_id: The batch ID to retry
            chunk_size: Maximum number of failed emails to retry
            
        Returns:
            Dict with retry results
        """
        # Find the organization ID for this batch
        org_id = self._get_org_id_for_batch(batch_id)
        if not org_id:
            raise ValueError(f"No batch found with ID: {batch_id}")
        
        conn = self.connect_to_org_db(org_id)
        cursor = conn.cursor()
        
        try:
            # Get failed emails for this batch (limited by chunk_size)
            cursor.execute(
                """
                SELECT id, org_id, contact_id, email_type, scheduled_date, send_mode, test_email
                FROM email_send_tracking
                WHERE batch_id = ? AND send_status = 'failed'
                ORDER BY scheduled_date
                LIMIT ?
                """,
                (batch_id, chunk_size)
            )
            
            failed_emails = cursor.fetchall()
            
            # Process results
            total_retries = len(failed_emails)
            successful_retries = 0
            failed_retries = 0
            errors = []
            
            # Process each email
            for email in failed_emails:
                email_id = email['id']
                contact_id = email['contact_id']
                email_type = email['email_type']
                scheduled_date = email['scheduled_date']
                send_mode = email['send_mode']
                test_email = email['test_email']
                
                try:
                    # Get contact details
                    contact = self._get_contact_details(org_id, contact_id)
                    if not contact:
                        raise ValueError(f"Contact {contact_id} not found")
                    
                    # Get email content
                    try:
                        email_date = datetime.strptime(scheduled_date, "%Y-%m-%d").date()
                        content = template_engine.render_email(email_type, contact, email_date)
                        html_content = template_engine.render_email(email_type, contact, email_date, html=True)
                    except Exception as render_error:
                        logger.error(f"Error rendering email: {render_error}")
                        raise ValueError(f"Failed to render {email_type} email: {str(render_error)[:100]}")
                    
                    # Determine recipient email
                    to_email = test_email if send_mode == 'test' else contact.get('email')
                    if not to_email:
                        raise ValueError(f"No recipient email address available")
                    
                    # Modify subject for test mode
                    subject = content['subject']
                    if send_mode == 'test':
                        subject = f"[TEST] {subject}"
                    
                    # Check if we're allowed to send emails in this mode according to app settings
                    can_send = self._can_send_in_mode(send_mode)
                    
                    # Both test and production modes send actual emails if enabled
                    # The only difference is the recipient and subject prefix
                    result = self.sendgrid_client.send_email(
                        to_email=to_email,
                        subject=subject,
                        content=content['body'],
                        html_content=html_content['html'],  # Extract just the HTML string from the dictionary
                        # Use dry_run if sending is disabled for this mode
                        dry_run=not can_send
                    )
                    
                    if result:
                        # Update record as sent
                        cursor.execute(
                            """
                            UPDATE email_send_tracking
                            SET send_status = 'sent', 
                                send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?
                            WHERE id = ?
                            """,
                            (datetime.now().isoformat(), email_id)
                        )
                        successful_retries += 1
                    else:
                        # Update record as failed
                        cursor.execute(
                            """
                            UPDATE email_send_tracking
                            SET send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?,
                                last_error = ?
                            WHERE id = ?
                            """,
                            (datetime.now().isoformat(), "Failed to send email (retry)", email_id)
                        )
                        failed_retries += 1
                        errors.append(f"Failed to send {email_type} email to contact {contact_id} (retry)")
                
                except Exception as e:
                    error_message = str(e)
                    # Update record as failed
                    cursor.execute(
                        """
                        UPDATE email_send_tracking
                        SET send_attempt_count = send_attempt_count + 1,
                            last_attempt_date = ?,
                            last_error = ?
                        WHERE id = ?
                        """,
                        (datetime.now().isoformat(), error_message[:500], email_id)
                    )
                    failed_retries += 1
                    errors.append(f"Error sending {email_type} email to contact {contact_id}: {error_message[:100]}")
                    logger.error(f"Error retrying email {email_id}: {e}")
            
            # Commit changes
            conn.commit()
            
            return {
                "retry_total": total_retries,
                "retry_successful": successful_retries,
                "retry_failed": failed_retries,
                "errors": errors
            }
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error retrying failed emails: {e}")
            raise
        finally:
            conn.close()
    
    async def retry_failed_emails_async(self, batch_id: str, chunk_size: int = 100, delay: float = 0) -> Dict[str, Any]:
        """
        Retry failed emails for a batch asynchronously.
        
        Args:
            batch_id: The batch ID to retry
            chunk_size: Maximum number of failed emails to retry
            delay: Optional delay between sending emails (in seconds)
            
        Returns:
            Dict with retry results
        """
        # Find the organization ID for this batch
        org_id = self._get_org_id_for_batch(batch_id)
        if not org_id:
            raise ValueError(f"No batch found with ID: {batch_id}")
        
        conn = await self.connect_to_org_db_async(org_id)
        
        try:
            # Get failed emails for this batch (limited by chunk_size)
            cursor = await conn.execute(
                """
                SELECT id, org_id, contact_id, email_type, scheduled_date, send_mode, test_email
                FROM email_send_tracking
                WHERE batch_id = ? AND send_status = 'failed'
                ORDER BY scheduled_date
                LIMIT ?
                """,
                (batch_id, chunk_size)
            )
            
            failed_emails = await cursor.fetchall()
            
            # Process results
            total_retries = len(failed_emails)
            successful_retries = 0
            failed_retries = 0
            errors = []
            
            # Process each email
            for email in failed_emails:
                email_id = email['id']
                contact_id = email['contact_id']
                email_type = email['email_type']
                scheduled_date = email['scheduled_date']
                send_mode = email['send_mode']
                test_email = email['test_email']
                
                try:
                    # Get contact details - Use the synchronous version for now
                    contact = self._get_contact_details(org_id, contact_id)
                    if not contact:
                        raise ValueError(f"Contact {contact_id} not found")
                    
                    # Get email content
                    try:
                        email_date = datetime.strptime(scheduled_date, "%Y-%m-%d").date()
                        content = template_engine.render_email(email_type, contact, email_date)
                        html_content = template_engine.render_email(email_type, contact, email_date, html=True)
                    except Exception as render_error:
                        logger.error(f"Error rendering email: {render_error}")
                        raise ValueError(f"Failed to render {email_type} email: {str(render_error)[:100]}")
                    
                    # Determine recipient email
                    to_email = test_email if send_mode == 'test' else contact.get('email')
                    if not to_email:
                        raise ValueError(f"No recipient email address available")
                    
                    # Modify subject for test mode
                    subject = content['subject']
                    if send_mode == 'test':
                        subject = f"[TEST] {subject}"
                    
                    # Check if we're allowed to send emails in this mode according to app settings
                    can_send = self._can_send_in_mode(send_mode)
                    
                    # Run the send_email in a thread to avoid blocking the event loop
                    result = await asyncio.to_thread(
                        self.sendgrid_client.send_email,
                        to_email=to_email,
                        subject=subject,
                        content=content['body'],
                        html_content=html_content['html'],  # Extract just the HTML string from the dictionary
                        dry_run=not can_send
                    )
                    
                    if result:
                        # Update record as sent
                        await conn.execute(
                            """
                            UPDATE email_send_tracking
                            SET send_status = 'sent', 
                                send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?
                            WHERE id = ?
                            """,
                            (datetime.now().isoformat(), email_id)
                        )
                        successful_retries += 1
                    else:
                        # Update record as failed
                        await conn.execute(
                            """
                            UPDATE email_send_tracking
                            SET send_attempt_count = send_attempt_count + 1,
                                last_attempt_date = ?,
                                last_error = ?
                            WHERE id = ?
                            """,
                            (datetime.now().isoformat(), "Failed to send email (retry)", email_id)
                        )
                        failed_retries += 1
                        errors.append(f"Failed to send {email_type} email to contact {contact_id} (retry)")
                
                except Exception as e:
                    error_message = str(e)
                    # Update record as failed
                    await conn.execute(
                        """
                        UPDATE email_send_tracking
                        SET send_attempt_count = send_attempt_count + 1,
                            last_attempt_date = ?,
                            last_error = ?
                        WHERE id = ?
                        """,
                        (datetime.now().isoformat(), error_message[:500], email_id)
                    )
                    failed_retries += 1
                    errors.append(f"Error sending {email_type} email to contact {contact_id}: {error_message[:100]}")
                    logger.error(f"Error retrying email {email_id}: {e}")
                
                # Add delay between sends if specified
                if delay > 0:
                    await asyncio.sleep(delay)
            
            # Commit changes
            await conn.commit()
            
            return {
                "retry_total": total_retries,
                "retry_successful": successful_retries,
                "retry_failed": failed_retries,
                "errors": errors
            }
        
        except Exception as e:
            await conn.rollback()
            logger.error(f"Error retrying failed emails: {e}")
            raise
        finally:
            await conn.close()
    
    def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """
        Get the status of a batch.
        
        Args:
            batch_id: The batch ID to get status for
            
        Returns:
            Dict with batch status information
        """
        # Find the organization ID for this batch
        org_id = self._get_org_id_for_batch(batch_id)
        if not org_id:
            raise ValueError(f"No batch found with ID: {batch_id}")
        
        conn = self.connect_to_org_db(org_id)
        cursor = conn.cursor()
        
        try:
            # Get batch statistics
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN send_status = 'sent' THEN 1 ELSE 0 END) as sent,
                    SUM(CASE WHEN send_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN send_status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN send_status = 'skipped' THEN 1 ELSE 0 END) as skipped,
                    MAX(created_at) as created_at,
                    MAX(updated_at) as updated_at,
                    MAX(send_mode) as send_mode,
                    MAX(test_email) as test_email
                FROM email_send_tracking
                WHERE batch_id = ?
                """,
                (batch_id,)
            )
            
            row = cursor.fetchone()
            
            if not row or row['total'] == 0:
                raise ValueError(f"No emails found for batch ID: {batch_id}")
            
            # Get organization name
            org_name = self._get_org_name(org_id)
            
            return {
                "batch_id": batch_id,
                "org_id": org_id,
                "org_name": org_name,
                "total": row['total'],
                "sent": row['sent'] or 0,
                "failed": row['failed'] or 0,
                "pending": row['pending'] or 0,
                "skipped": row['skipped'] or 0,
                "created_at": row['created_at'],
                "updated_at": row['updated_at'],
                "send_mode": row['send_mode'],
                "test_email": row['test_email'],
                "is_complete": (row['pending'] or 0) == 0
            }
        
        except Exception as e:
            logger.error(f"Error getting batch status: {e}")
            raise
        finally:
            conn.close()
    
    def list_batches(self, org_id: Optional[int] = None, limit: int = 20, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List recent email batches.
        
        Args:
            org_id: Optional organization ID to filter by
            limit: Maximum number of batches to return
            status: Optional status to filter by ('pending', 'sent', 'failed', or None for all)
            
        Returns:
            List of batch status dictionaries
        """
        if org_id is None:
            # Find batches across all organizations
            org_db_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "org_dbs")
            org_dbs = [f for f in os.listdir(org_db_dir) if f.startswith('org-') and f.endswith('.db')]
            
            all_batches = []
            for org_db in org_dbs:
                try:
                    org_id = int(org_db.replace('org-', '').replace('.db', ''))
                    org_batches = self._list_org_batches(org_id, limit, status)
                    all_batches.extend(org_batches)
                except Exception as e:
                    logger.error(f"Error listing batches for {org_db}: {e}")
            
            # Sort by created_at (newest first) and limit
            return sorted(all_batches, key=lambda b: b.get('created_at', ''), reverse=True)[:limit]
        else:
            # List batches for a specific organization
            return self._list_org_batches(org_id, limit, status)
    
    def _list_org_batches(self, org_id: int, limit: int = 20, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List batches for a specific organization.
        
        Args:
            org_id: Organization ID
            limit: Maximum number of batches to return
            status: Optional status to filter by ('pending', 'sent', 'failed', or None for all)
            
        Returns:
            List of batch info dictionaries
        """
        db_path = self.get_org_db_path(org_id)
        if not os.path.exists(db_path):
            logger.error(f"Organization database not found: {db_path}")
            return []
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Check if the email_send_tracking table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking'"
            )
            if not cursor.fetchone():
                logger.warning(f"email_send_tracking table not found in {db_path}")
                return []
            
            # Build the SQL query with optional status filter
            sql = """
                SELECT 
                    batch_id,
                    MAX(created_at) as created_at,
                    MAX(updated_at) as updated_at,
                    COUNT(*) as total,
                    SUM(CASE WHEN send_status = 'sent' THEN 1 ELSE 0 END) as sent,
                    SUM(CASE WHEN send_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN send_status = 'pending' THEN 1 ELSE 0 END) as pending,
                    MAX(send_mode) as send_mode
                FROM email_send_tracking
            """
            
            params = []
            
            # Add status filter if specified
            if status:
                # First, get all batch_ids that have emails with the specified status
                status_sql = """
                    WHERE batch_id IN (
                        SELECT DISTINCT batch_id 
                        FROM email_send_tracking 
                        WHERE send_status = ?
                    )
                """
                sql += status_sql
                params.append(status)
            
            # Complete the query
            sql += """
                GROUP BY batch_id
                ORDER BY created_at DESC
                LIMIT ?
            """
            params.append(limit)
            
            # Execute the query
            cursor.execute(sql, params)
            
            batches = []
            org_name = self._get_org_name(org_id)
            
            for row in cursor.fetchall():
                batches.append({
                    "batch_id": row['batch_id'],
                    "org_id": org_id,
                    "org_name": org_name,
                    "created_at": row['created_at'],
                    "updated_at": row['updated_at'],
                    "total": row['total'],
                    "sent": row['sent'] or 0,
                    "failed": row['failed'] or 0,
                    "pending": row['pending'] or 0,
                    "send_mode": row['send_mode'],
                    "is_complete": (row['pending'] or 0) == 0
                })
            
            return batches
        
        except Exception as e:
            logger.error(f"Error listing batches for organization {org_id}: {e}")
            return []
        finally:
            conn.close()
    
    def _get_org_id_for_batch(self, batch_id: str) -> Optional[int]:
        """Find the organization ID associated with a batch ID."""
        org_db_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "org_dbs")
        org_dbs = [f for f in os.listdir(org_db_dir) if f.startswith('org-') and f.endswith('.db')]
        
        for org_db in org_dbs:
            try:
                org_id = int(org_db.replace('org-', '').replace('.db', ''))
                db_path = os.path.join(org_db_dir, org_db)
                
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Check if the table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking'"
                )
                if not cursor.fetchone():
                    continue
                
                # Check if the batch exists in this org
                cursor.execute(
                    "SELECT COUNT(*) FROM email_send_tracking WHERE batch_id = ?",
                    (batch_id,)
                )
                count = cursor.fetchone()[0]
                conn.close()
                
                if count > 0:
                    return org_id
            except:
                continue
        
        return None
    
    def _get_org_name(self, org_id: int) -> str:
        """Get the organization name from the main database."""
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.db")
        if not os.path.exists(db_path):
            return f"Organization {org_id}"
        
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT name FROM organizations WHERE id = ?",
                (org_id,)
            )
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return row['name']
            else:
                return f"Organization {org_id}"
        except:
            return f"Organization {org_id}"
    
    def _can_send_in_mode(self, send_mode: str) -> bool:
        """
        Check if emails can be sent in the specified mode based on app settings.
        
        Args:
            send_mode: 'test' or 'production'
            
        Returns:
            True if emails can be sent in this mode, False otherwise
        """
        if send_mode == 'test':
            # Check if test email sending is enabled
            return os.environ.get("TEST_EMAIL_SENDING", "ENABLED").upper() == "ENABLED"
        elif send_mode == 'production':
            # Check if production email sending is enabled
            return os.environ.get("PRODUCTION_EMAIL_SENDING", "DISABLED").upper() == "ENABLED"
        else:
            # Unknown mode, default to not sending
            return False
    
    def _get_contact_details(self, org_id: int, contact_id: str) -> Optional[Dict[str, Any]]:
        """Get contact details from the organization database."""
        conn = self.connect_to_org_db(org_id)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                """
                SELECT id, first_name, last_name, email, state, birth_date, effective_date, zip_code
                FROM contacts
                WHERE id = ?
                """,
                (contact_id,)
            )
            
            contact = cursor.fetchone()
            
            if not contact:
                return None
            
            # Convert to dict
            contact_dict = dict(contact)
            
            # Add organization_id for compatibility with email templates
            contact_dict['organization_id'] = org_id
            
            # Prepare contact_info dict for compatibility with email templates
            contact_dict['contact_info'] = {
                'name': f"{contact_dict.get('first_name', '')} {contact_dict.get('last_name', '')}".strip(),
                'email': contact_dict.get('email', '')
            }
            
            return contact_dict
        
        except Exception as e:
            logger.error(f"Error getting contact details: {e}")
            return None
        finally:
            conn.close()