# Email Scheduler Database Migrations

This directory contains database migration scripts for the email scheduler application.

## Available Migrations

1. `add_email_tracking.sql` - Adds the `email_send_tracking` table to organization databases for tracking email sending status.

## How to Apply Migrations

There are three ways to apply migrations:

### 1. During the database dump and conversion process

When running `./dump_and_convert.sh`, migrations are automatically applied to all organization databases.

```bash
# Apply to all organizations
./dump_and_convert.sh

# Apply to a specific organization
./dump_and_convert.sh 37
```

### 2. Apply migrations to existing databases without re-dumping

If you want to apply migrations to existing databases without re-downloading from Turso:

```bash
# Apply to all organizations
./apply_migrations.sh

# Apply to a specific organization
./apply_migrations.sh 37
```

### 3. Apply a specific migration to a specific organization

If you want to apply just the email tracking migration to a specific organization:

```bash
./add_email_tracking.sh 37
```

## Adding New Migrations

1. Create a new SQL file in the `migrations` directory with a descriptive name (e.g., `add_new_feature.sql`)
2. Make sure your SQL file uses `IF NOT EXISTS` clauses for tables and indexes to prevent errors on reapplication
3. Commit the migration to the repository
4. Run one of the scripts above to apply the migration

## Email Tracking Table Structure

The `email_send_tracking` table has the following columns:

- `id`: INTEGER PRIMARY KEY AUTOINCREMENT
- `org_id`: INTEGER NOT NULL (references the organization ID)
- `contact_id`: TEXT NOT NULL (references the contact ID)
- `email_type`: TEXT NOT NULL (e.g., 'birthday', 'anniversary')
- `scheduled_date`: TEXT NOT NULL (ISO format, e.g., '2023-10-25T14:30:00Z')
- `send_status`: TEXT NOT NULL CHECK(send_status IN ('pending', 'sent', 'failed', 'skipped')) DEFAULT 'pending'
- `send_mode`: TEXT NOT NULL CHECK(send_mode IN ('test', 'production')) DEFAULT 'test'
- `test_email`: TEXT (stores the test email address if send_mode is 'test', NULL otherwise)
- `send_attempt_count`: INTEGER NOT NULL DEFAULT 0
- `last_attempt_date`: TEXT (ISO format timestamp of the last attempt, NULL until attempted)
- `last_error`: TEXT (error message if send_status is 'failed', NULL otherwise)
- `batch_id`: TEXT NOT NULL (unique identifier for the batch)
- `created_at`: TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
- `updated_at`: TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP

### Indexes for Performance

- Index on `batch_id` (for batch lookups)
- Index on `send_status` (for filtering pending/failed emails)
- Index on `send_mode` (for mode-specific queries)
- Index on `contact_id` (for contact-specific queries)
- Composite index on `contact_id` and `email_type` (for specific email types per contact)
- Composite index on `send_status` and `scheduled_date` (for date-filtered status queries)

### Auto-Updating Timestamp

A trigger `update_email_tracking_timestamp` automatically updates the `updated_at` field when a record is modified.