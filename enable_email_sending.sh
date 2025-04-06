#!/bin/bash
# Script to enable email sending by setting environment variables

# Color variables
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Print header
echo -e "${YELLOW}====================================================${NC}"
echo -e "${YELLOW}        Email Scheduler: Enable Email Sending        ${NC}"
echo -e "${YELLOW}====================================================${NC}"

# Disable dry run mode
echo -e "${YELLOW}This script will set the following environment variables:${NC}"
echo -e "1. EMAIL_DRY_RUN=false     - Disables dry run mode to send real emails"
echo -e "2. TEST_EMAIL_SENDING=ENABLED  - Allows test emails to be sent"
echo -e "3. PRODUCTION_EMAIL_SENDING  - Optional, allows real emails to recipients"

echo
echo -e "${RED}IMPORTANT: To apply these settings, you MUST run this script with 'source':${NC}"
echo -e "    ${GREEN}source enable_email_sending.sh${NC}"
echo -e "Running it normally (./enable_email_sending.sh) will NOT work!"
echo

read -p "Continue? (y/N): " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 1
fi

# Disable dry run mode
export EMAIL_DRY_RUN="false"
echo -e "${GREEN}✓${NC} Disabled dry run mode (EMAIL_DRY_RUN=false)"

# Enable test email sending
export TEST_EMAIL_SENDING="ENABLED"
echo -e "${GREEN}✓${NC} Enabled test email sending (TEST_EMAIL_SENDING=ENABLED)"

# Ask about production email sending
echo 
echo -e "${YELLOW}Do you want to enable production email sending?${NC}"
echo -e "${RED}WARNING:${NC} This will send real emails to actual recipients!"
read -p "Enable production emails? (y/N): " enable_production

if [[ "$enable_production" =~ ^[Yy]$ ]]; then
    export PRODUCTION_EMAIL_SENDING="ENABLED"
    echo -e "${RED}!${NC} ${RED}PRODUCTION EMAIL SENDING ENABLED${NC} (PRODUCTION_EMAIL_SENDING=ENABLED)"
else
    export PRODUCTION_EMAIL_SENDING="DISABLED"
    echo -e "${GREEN}✓${NC} Production email sending remains disabled"
fi

# Display current settings
echo 
echo -e "${YELLOW}Current Email Settings:${NC}"
echo -e "EMAIL_DRY_RUN: ${GREEN}$EMAIL_DRY_RUN${NC}"
echo -e "TEST_EMAIL_SENDING: ${GREEN}$TEST_EMAIL_SENDING${NC}"
echo -e "PRODUCTION_EMAIL_SENDING: ${GREEN}$PRODUCTION_EMAIL_SENDING${NC}"

# Check if SendGrid API key is set
if [ -z "$SENDGRID_API_KEY" ]; then
    echo 
    echo -e "${RED}WARNING:${NC} SENDGRID_API_KEY is not set!"
    echo -e "Please set your SendGrid API key to send actual emails:"
    echo -e "    export SENDGRID_API_KEY='your_api_key_here'"
else
    echo -e "SENDGRID_API_KEY: ${GREEN}Set${NC} (value hidden)"
fi

# Create a run command with environment variables
echo 
echo -e "${YELLOW}To run the app with these settings, use:${NC}"
echo -e "${GREEN}EMAIL_DRY_RUN=$EMAIL_DRY_RUN TEST_EMAIL_SENDING=$TEST_EMAIL_SENDING PRODUCTION_EMAIL_SENDING=$PRODUCTION_EMAIL_SENDING ./run_with_uv.sh [script] [args]${NC}"
echo 
echo -e "${YELLOW}====================================================${NC}"