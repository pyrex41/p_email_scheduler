#!/bin/bash

# Script to apply migrations to organization databases without re-dumping and converting

# Ensure necessary directories exist
mkdir -p migrations
mkdir -p org_dbs

# Colors for better readability
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if org ID was provided
if [ $# -eq 1 ]; then
    TARGET_ORG_ID=$1
    echo -e "${YELLOW}Processing only organization ID: ${TARGET_ORG_ID}${NC}"
fi

echo -e "${YELLOW}Starting migration application process...${NC}"

# Function to apply migrations to a SQLite database
apply_migrations() {
    local sqlite_file=$1
    
    echo -e "${YELLOW}Applying migrations to database: ${sqlite_file}...${NC}"
    
    # Apply all migration scripts in the migrations directory
    for migration_file in migrations/*.sql; do
        if [ -f "$migration_file" ]; then
            echo -e "${YELLOW}Applying migration: ${migration_file}...${NC}"
            if sqlite3 "${sqlite_file}" < "${migration_file}"; then
                echo -e "${GREEN}Successfully applied migration: ${migration_file}${NC}"
            else
                echo -e "${RED}Failed to apply migration: ${migration_file}${NC}"
                return 1
            fi
        fi
    done
    
    echo -e "${GREEN}Successfully applied all migrations to database: ${sqlite_file}${NC}"
    return 0
}

# Get list of organization databases to process
if [ -n "$TARGET_ORG_ID" ]; then
    # If TARGET_ORG_ID is provided, process only that org
    ORG_DBS=("org_dbs/org-${TARGET_ORG_ID}.db")
else
    # Otherwise, process all org databases
    ORG_DBS=(org_dbs/org-*.db)
fi

# Apply migrations to each organization database
for ORG_DB in "${ORG_DBS[@]}"; do
    if [ -f "$ORG_DB" ]; then
        # Extract org ID from filename
        ORG_ID=$(echo "$ORG_DB" | sed -E 's/org_dbs\/org-([0-9]+)\.db/\1/')
        
        echo -e "${YELLOW}Processing organization database: ${ORG_DB} (Org ID: ${ORG_ID})${NC}"
        
        # Apply migrations
        apply_migrations "${ORG_DB}"
    fi
done

echo -e "${GREEN}All migrations have been applied!${NC}"