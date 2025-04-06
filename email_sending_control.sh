#!/bin/bash
# Script to run the app with specific email sending settings

# Color variables
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print header
echo -e "${YELLOW}====================================================${NC}"
echo -e "${YELLOW}      Email Scheduler: Email Sending Control        ${NC}"
echo -e "${YELLOW}====================================================${NC}"

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo -e "${GREEN}Loading variables from .env file...${NC}"
    set -a  # automatically export all variables
    source .env
    set +a  # stop automatically exporting
else
    echo -e "${YELLOW}No .env file found. You may want to create one using .env.example as a template.${NC}"
fi

# Function to display help
show_help() {
    echo -e "Usage: $0 [options] [command]"
    echo -e ""
    echo -e "Options:"
    echo -e "  --dry-run           Run in dry-run mode (no emails sent)"
    echo -e "  --test-only         Send only to test email addresses"
    echo -e "  --live              Send real emails to recipients (caution!)"
    echo -e "  --help              Show this help message"
    echo -e ""
    echo -e "Commands:"
    echo -e "  run <script>        Run a specific script with email settings"
    echo -e "  server              Start the web server"
    echo -e "  set                 Just set the environment variables for this session"
    echo -e ""
    echo -e "Examples:"
    echo -e "  $0 --test-only server               Start the web server sending only to test addresses"
    echo -e "  $0 --live run email_batch_manager.py  Run with live email sending"
    echo -e ""
}

# Function to display current settings
show_settings() {
    echo -e "${BLUE}Current Email Settings:${NC}"
    
    # EMAIL_DRY_RUN setting
    if [ "${EMAIL_DRY_RUN}" == "false" ]; then
        echo -e "EMAIL_DRY_RUN     = ${GREEN}false${NC} (emails will be sent)"
    else
        echo -e "EMAIL_DRY_RUN     = ${YELLOW}true${NC} (dry run, no emails sent)"
    fi
    
    # Test mode setting
    if [ "${TEST_EMAIL_SENDING}" == "ENABLED" ]; then
        echo -e "TEST_EMAIL_SENDING = ${GREEN}ENABLED${NC} (test mode can send emails)"
    else
        echo -e "TEST_EMAIL_SENDING = ${YELLOW}DISABLED${NC} (test mode won't send emails)"
    fi
    
    # Production mode setting
    if [ "${PRODUCTION_EMAIL_SENDING}" == "ENABLED" ]; then
        echo -e "PRODUCTION_EMAIL_SENDING = ${RED}ENABLED${NC} (production mode sends to real recipients)"
    else
        echo -e "PRODUCTION_EMAIL_SENDING = ${YELLOW}DISABLED${NC} (production mode won't send emails)"
    fi
    
    # Check if SendGrid API key is set
    if [ "$EMAIL_DRY_RUN" == "false" ] && [ -z "$SENDGRID_API_KEY" ]; then
        echo -e "${RED}WARNING: SENDGRID_API_KEY is not set! Real emails cannot be sent.${NC}"
    elif [ "$EMAIL_DRY_RUN" == "false" ]; then
        echo -e "SENDGRID_API_KEY is set (value hidden)"
    fi
}

# Parse command line arguments
MODE="--dry-run"  # Default mode
COMMAND=""
ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run|--test-only|--live)
            MODE="$1"
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        run|server|set)
            COMMAND="$1"
            shift
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

# Set environment variables based on mode
case $MODE in
    --dry-run)
        export EMAIL_DRY_RUN="true"
        export TEST_EMAIL_SENDING="ENABLED"
        export PRODUCTION_EMAIL_SENDING="DISABLED"
        echo -e "${GREEN}✓${NC} Running in ${YELLOW}DRY RUN${NC} mode (no emails will be sent)"
        ;;
    --test-only)
        export EMAIL_DRY_RUN="false"
        export TEST_EMAIL_SENDING="ENABLED"
        export PRODUCTION_EMAIL_SENDING="DISABLED"
        echo -e "${GREEN}✓${NC} Running in ${YELLOW}TEST ONLY${NC} mode (emails sent only to test addresses)"
        ;;
    --live)
        export EMAIL_DRY_RUN="false"
        export TEST_EMAIL_SENDING="ENABLED"
        export PRODUCTION_EMAIL_SENDING="ENABLED"
        echo -e "${RED}!${NC} Running in ${RED}LIVE${NC} mode (real emails will be sent to recipients)"
        echo -e "${RED}!${NC} ${RED}WARNING: REAL EMAILS WILL BE SENT TO ACTUAL RECIPIENTS!${NC}"
        read -p "Are you sure you want to continue? (y/N): " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            echo "Cancelled."
            exit 1
        fi
        ;;
esac

# Check if SendGrid API key is set when not in dry run mode
if [ "$EMAIL_DRY_RUN" == "false" ] && [ -z "$SENDGRID_API_KEY" ]; then
    echo -e "${RED}WARNING: SENDGRID_API_KEY is not set!${NC}"
    echo -e "Please set your SendGrid API key to send actual emails:"
    echo -e "    export SENDGRID_API_KEY='your_api_key_here'"
    
    # Only exit if we're trying to send emails
    if [ "$COMMAND" != "set" ]; then
        echo -e "${RED}Cannot continue without SendGrid API key when not in dry run mode.${NC}"
        exit 1
    fi
fi

# Display current settings
show_settings

# Handle different commands
case $COMMAND in
    "run")
        if [ ${#ARGS[@]} -eq 0 ]; then
            echo -e "${RED}Error: No script specified for 'run' command${NC}"
            show_help
            exit 1
        fi
        
        echo 
        echo -e "${YELLOW}Running script: ${ARGS[0]}${NC}"
        echo -e "${YELLOW}====================================================${NC}"
        
        # Run the specified script with environment variables set
        EMAIL_DRY_RUN=$EMAIL_DRY_RUN \
        TEST_EMAIL_SENDING=$TEST_EMAIL_SENDING \
        PRODUCTION_EMAIL_SENDING=$PRODUCTION_EMAIL_SENDING \
        ./run_with_uv.sh "${ARGS[@]}"
        ;;
        
    "server")
        echo 
        echo -e "${YELLOW}Starting web server...${NC}"
        echo -e "${YELLOW}====================================================${NC}"
        
        # Start the web server with environment variables set
        EMAIL_DRY_RUN=$EMAIL_DRY_RUN \
        TEST_EMAIL_SENDING=$TEST_EMAIL_SENDING \
        PRODUCTION_EMAIL_SENDING=$PRODUCTION_EMAIL_SENDING \
        uvicorn app:app --reload --host 0.0.0.0 --port 8000
        ;;
        
    "set")
        echo 
        echo -e "${YELLOW}Environment variables have been set for this shell session.${NC}"
        echo -e "${YELLOW}To run commands with these settings:${NC}"
        echo -e "EMAIL_DRY_RUN=$EMAIL_DRY_RUN TEST_EMAIL_SENDING=$TEST_EMAIL_SENDING PRODUCTION_EMAIL_SENDING=$PRODUCTION_EMAIL_SENDING ./run_with_uv.sh [script]"
        echo -e "${YELLOW}====================================================${NC}"
        ;;
        
    "")
        echo 
        echo -e "${RED}Error: No command specified${NC}"
        show_help
        exit 1
        ;;
        
    *)
        echo -e "${RED}Error: Unknown command '$COMMAND'${NC}"
        show_help
        exit 1
        ;;
esac