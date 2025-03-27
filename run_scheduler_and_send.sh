#!/bin/bash

# run_scheduler_and_send.sh - Run the email scheduler and send emails
#
# This script runs the optimized email scheduler and then sends the resulting emails
# using SendGrid. It supports both dry-run and live modes.

set -e  # Exit on error

# Default values
INPUT_FILE="./temp_test/contacts.json"
OUTPUT_FILE="./output_dir/scheduled_emails.json"
START_DATE=$(date +%Y-%m-%d)
END_DATE=$(date -v+365d +%Y-%m-%d 2>/dev/null || date --date="+365 days" +%Y-%m-%d 2>/dev/null || date -d "+365 days" +%Y-%m-%d)
ASYNC="false"
LIVE="false"
MAX_EMAILS=0
DELAY=0.5  # Default delay between emails in seconds

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --input)
      INPUT_FILE="$2"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --start-date)
      START_DATE="$2"
      shift 2
      ;;
    --end-date)
      END_DATE="$2"
      shift 2
      ;;
    --async)
      ASYNC="true"
      shift
      ;;
    --live)
      LIVE="true"
      shift
      ;;
    --max-emails)
      MAX_EMAILS="$2"
      shift 2
      ;;
    --delay)
      DELAY="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --input FILE       Input JSON file with contacts (default: $INPUT_FILE)"
      echo "  --output FILE      Output JSON file for scheduled emails (default: $OUTPUT_FILE)"
      echo "  --start-date DATE  Start date for scheduling (YYYY-MM-DD, default: today)"
      echo "  --end-date DATE    End date for scheduling (YYYY-MM-DD, default: today + 365 days)"
      echo "  --async            Use asynchronous processing (default: false)"
      echo "  --live             Send actual emails (default: dry-run mode)"
      echo "  --max-emails NUM   Maximum number of emails to send (default: 0 = no limit)"
      echo "  --delay SEC        Delay between emails in seconds (default: 0.5)"
      echo "  --help             Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Create output directory if it doesn't exist
mkdir -p "$(dirname "$OUTPUT_FILE")"

# Ensure UV is available
if ! command -v uv &> /dev/null; then
    echo "UV is not installed or not in PATH. Please install it first."
    exit 1
fi

# Print configuration
echo "Email Scheduler Pipeline"
echo "======================="
echo "Input file:     $INPUT_FILE"
echo "Output file:    $OUTPUT_FILE"
echo "Date range:     $START_DATE to $END_DATE"
echo "Async mode:     $ASYNC"
echo "Live mode:      $LIVE"
echo "Max emails:     $MAX_EMAILS"
echo "Email delay:    $DELAY seconds"
echo ""

# Step 1: Run the scheduler
echo "Step 1: Running email scheduler..."
if [ "$ASYNC" == "true" ]; then
    # Run with async mode
    ./run_with_uv.sh email_scheduler_optimized.py --input "$INPUT_FILE" --output "$OUTPUT_FILE" --start-date "$START_DATE" --end-date "$END_DATE" --async
else
    # Run in sync mode
    ./run_with_uv.sh email_scheduler_optimized.py --input "$INPUT_FILE" --output "$OUTPUT_FILE" --start-date "$START_DATE" --end-date "$END_DATE"
fi

# Check if scheduler was successful
if [ $? -ne 0 ]; then
    echo "Error: Email scheduler failed"
    exit 1
fi

echo ""
echo "Step 2: Sending emails..."

# Build command for sending emails
SEND_CMD="./run_with_uv.sh send_scheduled_emails.py --input \"$OUTPUT_FILE\" --contacts \"$INPUT_FILE\" --start-date \"$START_DATE\" --end-date \"$END_DATE\" --delay \"$DELAY\""

# Add limit if specified
if [ "$MAX_EMAILS" -gt 0 ]; then
    SEND_CMD="$SEND_CMD --limit $MAX_EMAILS"
fi

# Add live mode if specified
if [ "$LIVE" == "true" ]; then
    SEND_CMD="$SEND_CMD --live"
fi

# Run the send command
eval $SEND_CMD

# Check if sending was successful
if [ $? -ne 0 ]; then
    echo "Error: Email sending failed"
    exit 1
fi

echo ""
echo "Pipeline completed successfully"