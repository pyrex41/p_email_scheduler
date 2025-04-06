-- Migration script to add email_send_tracking table and indexes to organization databases
-- This script can be applied to any organization database

-- Create the email_send_tracking table if it doesn't exist
CREATE TABLE IF NOT EXISTS email_send_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    org_id INTEGER NOT NULL,
    contact_id TEXT NOT NULL,
    email_type TEXT NOT NULL,
    scheduled_date TEXT NOT NULL,
    send_status TEXT NOT NULL CHECK(send_status IN ('pending', 'processing', 'accepted', 'delivered', 'sent', 'deferred', 'bounced', 'dropped', 'failed', 'skipped')) DEFAULT 'pending',
    send_mode TEXT NOT NULL CHECK(send_mode IN ('test', 'production')) DEFAULT 'test',
    test_email TEXT,
    send_attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_date TEXT,
    last_error TEXT,
    batch_id TEXT NOT NULL,
    message_id TEXT,
    delivery_status TEXT,
    status_checked_at TEXT,
    status_details TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for the email_send_tracking table if they don't exist
-- We need to check if indexes exist before creating them to avoid errors

-- Index on batch_id for batch operations and lookups
CREATE INDEX IF NOT EXISTS idx_email_tracking_batch_id ON email_send_tracking(batch_id);

-- Index on send_status for filtering emails by status (pending/sent/failed/skipped)
CREATE INDEX IF NOT EXISTS idx_email_tracking_send_status ON email_send_tracking(send_status);

-- Index on send_mode for filtering by mode (test/production)
CREATE INDEX IF NOT EXISTS idx_email_tracking_send_mode ON email_send_tracking(send_mode);

-- Index on contact_id for looking up emails by contact
CREATE INDEX IF NOT EXISTS idx_email_tracking_contact_id ON email_send_tracking(contact_id);

-- Composite index on contact_id and email_type for retrieving specific email types for a contact
CREATE INDEX IF NOT EXISTS idx_email_tracking_contact_type ON email_send_tracking(contact_id, email_type);

-- Composite index on send_status and scheduled_date for retrieving emails by status within a date range
CREATE INDEX IF NOT EXISTS idx_email_tracking_status_date ON email_send_tracking(send_status, scheduled_date);

-- Index on message_id for looking up emails by SendGrid message ID
CREATE INDEX IF NOT EXISTS idx_email_tracking_message_id ON email_send_tracking(message_id);

-- Index on delivery_status for filtering by delivery status
CREATE INDEX IF NOT EXISTS idx_email_tracking_delivery_status ON email_send_tracking(delivery_status);

-- Create a trigger to update the updated_at timestamp when a record is modified
CREATE TRIGGER IF NOT EXISTS update_email_tracking_timestamp 
AFTER UPDATE ON email_send_tracking 
BEGIN
    UPDATE email_send_tracking SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id; 
END;