#!/bin/bash

# Script to add email tracking table to a specific organization database

# Colors for better readability
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if org ID was provided
if [ $# -ne 1 ]; then
    echo -e "${RED}Error: Organization ID is required${NC}"
    echo -e "Usage: $0 <org_id>"
    exit 1
fi

ORG_ID=$1
ORG_DB="org_dbs/org-${ORG_ID}.db"

echo -e "${YELLOW}Adding email tracking table to organization database: ${ORG_DB}${NC}"

# Check if the database exists
if [ ! -f "${ORG_DB}" ]; then
    echo -e "${RED}Error: Database file not found: ${ORG_DB}${NC}"
    exit 1
fi

# Ensure migrations directory exists
mkdir -p migrations

# Apply the email tracking migration
echo -e "${YELLOW}Applying email tracking migration...${NC}"
if sqlite3 "${ORG_DB}" < "migrations/add_email_tracking.sql"; then
    echo -e "${GREEN}Successfully added email tracking table to ${ORG_DB}${NC}"
    # Verify the table was created
    TABLE_EXISTS=$(sqlite3 "${ORG_DB}" "SELECT name FROM sqlite_master WHERE type='table' AND name='email_send_tracking';")
    if [ -n "${TABLE_EXISTS}" ]; then
        echo -e "${GREEN}Verified email_send_tracking table exists in ${ORG_DB}${NC}"
        exit 0
    else
        echo -e "${RED}Failed to verify email_send_tracking table in ${ORG_DB}${NC}"
        exit 1
    fi
else
    echo -e "${RED}Failed to add email tracking table to ${ORG_DB}${NC}"
    exit 1
fi