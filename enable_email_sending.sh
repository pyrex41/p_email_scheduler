#!/bin/bash

# enable_email_sending.sh - Script to show how to enable real email sending
#
# IMPORTANT: By default, all emails are sent in "dry run" mode to prevent
# accidental production emails. To enable actual email sending, you need to:
#
# 1. Set the EMAIL_SENDING_ENABLED environment variable to "ENABLED"
# 2. Use production mode in the web interface (not test mode)
#
# This script demonstrates how to do that.

# Print warning
echo "=============================================================="
echo "WARNING: This script shows how to enable REAL email sending!"
echo "By default the system is in dry-run mode and no real emails"
echo "will be sent regardless of settings in the web interface."
echo "=============================================================="
echo ""
echo "To enable REAL email sending, run the app like this:"
echo ""
echo "    EMAIL_SENDING_ENABLED=ENABLED uvicorn app:app --reload"
echo ""
echo "Then select 'production' mode in the send emails page."
echo ""
echo "IMPORTANT: When in dry-run mode (default), the system will log"
echo "emails but not actually send them to recipients."
echo ""
echo "Current settings:"
echo "----------------"

# Check current environment variable
if [ -z "${EMAIL_SENDING_ENABLED}" ]; then
    echo "EMAIL_SENDING_ENABLED is not set (default: DISABLED)"
    echo "Status: DRY RUN MODE - NO EMAILS WILL BE SENT"
else
    if [ "${EMAIL_SENDING_ENABLED}" == "ENABLED" ]; then
        echo "EMAIL_SENDING_ENABLED = ${EMAIL_SENDING_ENABLED}"
        echo "Status: PRODUCTION MODE - REAL EMAILS WILL BE SENT"
    else
        echo "EMAIL_SENDING_ENABLED = ${EMAIL_SENDING_ENABLED}"
        echo "Status: DRY RUN MODE - NO EMAILS WILL BE SENT"
    fi
fi

# Remind about dry run and test mode
echo ""
echo "Remember: Even with EMAIL_SENDING_ENABLED=ENABLED, emails will"
echo "still be in dry-run mode if you select 'test' mode in the interface."
echo "To actually send emails you must select 'production' mode."