#!/bin/bash

# email_sending_control.sh - Script to control email sending modes
#
# This system has two separate controls for email sending:
#
# 1. TEST_EMAIL_SENDING - Controls sending real emails in test mode
#    Default: ENABLED - Test mode will send real emails to test addresses
#
# 2. PRODUCTION_EMAIL_SENDING - Controls sending real emails in production mode
#    Default: DISABLED - Production mode won't send real emails to actual recipients
#
# This script shows how to control these settings.

# Print header
echo "=============================================================="
echo "EMAIL SENDING CONTROL PANEL"
echo "=============================================================="
echo ""

# Function to display current settings
show_settings() {
  echo "Current Settings:"
  echo "----------------"
  # Test mode setting
  if [ -z "${TEST_EMAIL_SENDING}" ] || [ "${TEST_EMAIL_SENDING}" == "ENABLED" ]; then
    echo "TEST_EMAIL_SENDING = ENABLED (default)"
    echo "  • Test mode WILL send real emails to test addresses"
  else
    echo "TEST_EMAIL_SENDING = DISABLED"
    echo "  • Test mode will NOT send any emails (dry run only)"
  fi
  
  # Production mode setting
  if [ -z "${PRODUCTION_EMAIL_SENDING}" ] || [ "${PRODUCTION_EMAIL_SENDING}" == "DISABLED" ]; then
    echo "PRODUCTION_EMAIL_SENDING = DISABLED (default)"
    echo "  • Production mode will NOT send any emails (dry run only)"
  else
    echo "PRODUCTION_EMAIL_SENDING = ENABLED"
    echo "  • Production mode WILL send real emails to actual recipients"
    echo "  • CAUTION: This will send emails to real contact email addresses!"
  fi
}

# Show current settings
show_settings

# Display usage instructions
echo ""
echo "Usage Examples:"
echo "----------------"
echo "1. Default recommended configuration (send test emails only):"
echo "   $ uvicorn app:app --reload"
echo ""
echo "2. Disable all email sending (including test emails):"
echo "   $ TEST_EMAIL_SENDING=DISABLED uvicorn app:app --reload"
echo ""
echo "3. Enable production email sending (CAUTION!):"
echo "   $ PRODUCTION_EMAIL_SENDING=ENABLED uvicorn app:app --reload"
echo ""
echo "4. Custom configuration:"
echo "   $ TEST_EMAIL_SENDING=DISABLED PRODUCTION_EMAIL_SENDING=ENABLED uvicorn app:app --reload"
echo ""
echo "Remember: The web interface still controls whether you're in test or production"
echo "mode. These environment variables only control whether emails are actually sent"
echo "in each respective mode."