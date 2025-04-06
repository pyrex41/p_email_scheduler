#!/usr/bin/env python
"""
Update Email Tracking Script

This script checks and updates the delivery status of emails in the database.
It can be run as a standalone script or imported and used programmatically.
"""

import os
import sys
import argparse
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("update_email_tracking")

def get_org_db_paths() -> List[str]:
    """Get paths to all organization databases."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    org_db_dir = os.path.join(base_dir, "org_dbs")
    
    if not os.path.exists(org_db_dir):
        logger.warning(f"Organization database directory not found: {org_db_dir}")
        return []
    
    db_paths = []
    for filename in os.listdir(org_db_dir):
        if filename.startswith('org-') and filename.endswith('.db'):
            db_paths.append(os.path.join(org_db_dir, filename))
    
    return db_paths

def get_org_id_from_db_path(db_path: str) -> Optional[int]:
    """Extract organization ID from database path."""
    try:
        filename = os.path.basename(db_path)
        return int(filename.replace('org-', '').replace('.db', ''))
    except (ValueError, IndexError):
        logger.error(f"Could not extract organization ID from {db_path}")
        return None

def update_batch_status(db_path: str, batch_id: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    """
    Update email status for a specific batch or all pending emails.
    
    Args:
        db_path: Path to organization database
        batch_id: Optional batch ID to filter by
        limit: Maximum number of emails to process
        
    Returns:
        Dictionary with update results
    """
    from email_status_checker import update_batch_statuses
    
    org_id = get_org_id_from_db_path(db_path)
    if not org_id:
        return {"success": False, "error": f"Invalid database path: {db_path}"}
    
    result = update_batch_statuses(org_id, batch_id, limit)
    return result

def update_all_organizations(batch_id: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    """
    Update email status across all organizations.
    
    Args:
        batch_id: Optional batch ID to filter by
        limit: Maximum number of emails to process per organization
        
    Returns:
        Dictionary with update results by organization
    """
    db_paths = get_org_db_paths()
    results = {}
    
    for db_path in db_paths:
        org_id = get_org_id_from_db_path(db_path)
        if not org_id:
            continue
            
        try:
            result = update_batch_status(db_path, batch_id, limit)
            results[str(org_id)] = result
            
            # Log results
            if result.get("success", False):
                stats = result.get("stats", {})
                logger.info(f"Organization {org_id}: Updated {stats.get('checked', 0)} emails, found {stats.get('delivered', 0)} delivered")
            else:
                logger.error(f"Organization {org_id}: Error updating status - {result.get('error')}")
        
        except Exception as e:
            logger.error(f"Error updating organization {org_id}: {e}")
            results[str(org_id)] = {"success": False, "error": str(e)}
    
    return {
        "success": True,
        "organizations_processed": len(results),
        "results": results
    }

def batch_mark_as_delivered(org_id: int, batch_id: str) -> Dict[str, Any]:
    """
    Mark all sent emails in a batch as delivered.
    This is useful when webhook events aren't available or aren't working.
    
    Args:
        org_id: Organization ID
        batch_id: Batch ID
        
    Returns:
        Dictionary with update results
    """
    try:
        # Connect to database
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"org_dbs/org-{org_id}.db")
        if not os.path.exists(db_path):
            return {"success": False, "error": f"Organization database not found: {db_path}"}
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if the email_send_tracking table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking'")
        if not cursor.fetchone():
            return {"success": False, "error": f"Table email_send_tracking not found in {db_path}"}
        
        # Get count of sent emails
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM email_send_tracking 
            WHERE batch_id = ? AND send_status = 'sent' AND (delivery_status != 'delivered' OR delivery_status IS NULL)
            """,
            (batch_id,)
        )
        sent_count = cursor.fetchone()[0]
        
        # If no sent emails, nothing to update
        if sent_count == 0:
            return {"success": True, "updated": 0, "message": "No sent emails to mark as delivered"}
        
        # Update sent emails to delivered
        cursor.execute(
            """
            UPDATE email_send_tracking
            SET send_status = 'delivered', 
                delivery_status = 'delivered',
                status_checked_at = ?
            WHERE batch_id = ? AND send_status = 'sent' AND (delivery_status != 'delivered' OR delivery_status IS NULL)
            """,
            (datetime.now().isoformat(), batch_id)
        )
        conn.commit()
        
        # Get updated count
        updated_count = cursor.rowcount
        
        # Close connection
        conn.close()
        
        return {
            "success": True,
            "updated": updated_count,
            "message": f"Marked {updated_count} sent emails as delivered"
        }
        
    except Exception as e:
        logger.error(f"Error marking emails as delivered: {e}")
        return {"success": False, "error": str(e)}

def get_pending_batches(db_path: str) -> List[Dict[str, Any]]:
    """
    Get list of batches with pending emails.
    
    Args:
        db_path: Path to organization database
        
    Returns:
        List of batch dictionaries
    """
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check if the email_send_tracking table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking'")
        if not cursor.fetchone():
            logger.warning(f"Table email_send_tracking not found in {db_path}")
            return []
        
        # Get batches with pending emails
        cursor.execute(
            """
            SELECT 
                batch_id,
                COUNT(*) as total,
                SUM(CASE WHEN send_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN send_status IN ('sent', 'accepted') THEN 1 ELSE 0 END) as sent,
                SUM(CASE WHEN send_status = 'delivered' THEN 1 ELSE 0 END) as delivered,
                SUM(CASE WHEN send_status = 'failed' THEN 1 ELSE 0 END) as failed,
                MAX(last_attempt_date) as last_attempt
            FROM email_send_tracking
            GROUP BY batch_id
            HAVING pending > 0 OR (sent > 0 AND delivered = 0)
            ORDER BY last_attempt DESC
            """
        )
        
        batches = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return batches
    
    except Exception as e:
        logger.error(f"Error getting pending batches from {db_path}: {e}")
        return []

def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Update email delivery status in the database")
    parser.add_argument("--batch-id", help="Update emails for a specific batch ID")
    parser.add_argument("--org-id", type=int, help="Update emails for a specific organization ID")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of emails to process per organization")
    parser.add_argument("--list-pending", action="store_true", help="List batches with pending emails")
    parser.add_argument("--mark-delivered", action="store_true", help="Mark all sent emails as delivered (when used with --batch-id and --org-id)")
    args = parser.parse_args()
    
    if args.mark_delivered:
        if not args.batch_id or not args.org_id:
            print("Error: --batch-id and --org-id are required with --mark-delivered")
            return 1
            
        result = batch_mark_as_delivered(args.org_id, args.batch_id)
        
        if result.get("success", False):
            print(result.get("message"))
            return 0
        else:
            print(f"Error: {result.get('error')}")
            return 1
    
    if args.list_pending:
        # List batches with pending emails
        db_paths = get_org_db_paths()
        
        if args.org_id:
            # Filter to specific organization
            db_paths = [p for p in db_paths if get_org_id_from_db_path(p) == args.org_id]
        
        for db_path in db_paths:
            org_id = get_org_id_from_db_path(db_path)
            pending_batches = get_pending_batches(db_path)
            
            if pending_batches:
                print(f"\nOrganization {org_id}: {len(pending_batches)} batches with pending emails")
                for batch in pending_batches:
                    print(f"  Batch {batch['batch_id']}: {batch['pending']} pending, {batch['sent']} sent, {batch['delivered']} delivered")
            else:
                print(f"\nOrganization {org_id}: No pending batches")
    
    else:
        # Update email statuses
        if args.org_id:
            # Update specific organization
            db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"org_dbs/org-{args.org_id}.db")
            if not os.path.exists(db_path):
                print(f"Error: Organization database not found: {db_path}")
                return 1
                
            result = update_batch_status(db_path, args.batch_id, args.limit)
            
            if result.get("success", False):
                stats = result.get("stats", {})
                print(f"Organization {args.org_id}:")
                print(f"  Updated: {stats.get('checked', 0)}")
                print(f"  Delivered: {stats.get('delivered', 0)}")
                print(f"  Pending: {stats.get('needs_check', 0)}")
            else:
                print(f"Error updating organization {args.org_id}: {result.get('error')}")
        
        else:
            # Update all organizations
            print("Updating all organizations...")
            result = update_all_organizations(args.batch_id, args.limit)
            
            if result.get("success", False):
                print(f"Processed {result.get('organizations_processed', 0)} organizations")
            else:
                print(f"Error updating organizations: {result.get('error')}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())