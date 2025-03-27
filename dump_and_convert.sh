#!/bin/bash

# Ensure necessary directories exist
mkdir -p dumps
mkdir -p org_dbs
mkdir -p output_dir

# Load environment variables
source .env

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

echo -e "${YELLOW}Starting database dump and conversion process...${NC}"

# Function to normalize database URL
normalize_url() {
    local url=$1
    # Remove libsql:// or https:// prefix
    url=${url#libsql://}
    url=${url#https://}
    echo "$url"
}

# Function to dump a Turso database
dump_turso_db() {
    local url=$1
    local token=$2
    local output_file=$3
    
    echo -e "${YELLOW}Dumping database from ${url} to ${output_file}...${NC}"
    
    # Normalize the URL
    local base_url=$(normalize_url "$url")
    
    # Use curl to get the dump
    if curl -s -X GET "https://${base_url}/dump" \
         -H "Authorization: Bearer ${token}" \
         -o "${output_file}"; then
        echo -e "${GREEN}Successfully dumped database to ${output_file}${NC}"
        return 0
    else
        echo -e "${RED}Failed to dump database from ${url}${NC}"
        return 1
    fi
}

# Function to convert a SQL dump to SQLite
convert_to_sqlite() {
    local dump_file=$1
    local sqlite_file=$2
    
    echo -e "${YELLOW}Converting ${dump_file} to SQLite database ${sqlite_file}...${NC}"
    
    # Remove any existing database file
    rm -f "${sqlite_file}"
    
    # Create SQLite database from dump
    if sqlite3 "${sqlite_file}" < "${dump_file}"; then
        echo -e "${GREEN}Successfully converted to SQLite database: ${sqlite_file}${NC}"
        return 0
    else
        echo -e "${RED}Failed to convert to SQLite database: ${sqlite_file}${NC}"
        return 1
    fi
}

# Function to extract database ID from URL
extract_db_id() {
    local url=$1
    # Extract the org-X part from the URL
    if [[ $url =~ org-([0-9]+) ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo ""
    fi
}

# Step 1: Dump the medicare-portal database
MEDICARE_PORTAL_DUMP="dumps/medicare-portal.sql"
dump_turso_db "${TURSO_PY_DB_URL}" "${TURSO_PY_AUTH_TOKEN}" "${MEDICARE_PORTAL_DUMP}"

# Step 2: Convert the medicare-portal dump to SQLite
convert_to_sqlite "${MEDICARE_PORTAL_DUMP}" "main.db"

# Step 3: Get organization database URLs and tokens
echo -e "${YELLOW}Extracting organization database URLs and tokens...${NC}"
if [ -n "$TARGET_ORG_ID" ]; then
    ORG_DATA=$(sqlite3 main.db "SELECT id, name, turso_db_url, turso_auth_token FROM organizations WHERE turso_db_url IS NOT NULL AND id = ${TARGET_ORG_ID};")
else
    ORG_DATA=$(sqlite3 main.db "SELECT id, name, turso_db_url, turso_auth_token FROM organizations WHERE turso_db_url IS NOT NULL;")
fi

# Step 4: Dump each organization database and convert to SQLite
echo -e "${YELLOW}Processing organization databases...${NC}"
echo "${ORG_DATA}" | while IFS='|' read -r org_id name url token; do
    # Skip if any field is empty
    if [[ -z "$org_id" || -z "$url" || -z "$token" ]]; then
        continue
    fi
    
    # Extract the actual database ID from the URL
    db_id=$(extract_db_id "$url")
    if [[ -z "$db_id" ]]; then
        echo -e "${RED}Could not extract database ID from URL: ${url}${NC}"
        continue
    fi
    
    # Clean up name for filename (remove spaces and special characters)
    clean_name=$(echo "${name}" | tr ' ' '_' | tr -cd '[:alnum:]_-')
    
    # Dump the organization database using the URL's database ID
    ORG_DUMP="dumps/org-${db_id}-${clean_name}.sql"
    dump_turso_db "${url}" "${token}" "${ORG_DUMP}"
    
    # Convert to SQLite using the organization's actual ID from the database
    ORG_DB="org_dbs/org-${org_id}.db"
    convert_to_sqlite "${ORG_DUMP}" "${ORG_DB}"
    
    echo -e "${GREEN}Processed organization ${org_id} (${name}) using database ${db_id}${NC}"
done

echo -e "${GREEN}All databases have been dumped and converted!${NC}"
echo -e "${YELLOW}Summary:${NC}"
echo -e "- Medicare Portal database: main.db"
if [ -n "$TARGET_ORG_ID" ]; then
    echo -e "- Organization database: org_dbs/org-${TARGET_ORG_ID}.db"
else
    echo -e "- Organization databases: org_dbs/org-*.db"
fi
rm -rf dumps