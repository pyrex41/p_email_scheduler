#!/bin/bash

# Disable history expansion to prevent issues with ! in arguments
set +H

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    set -a # automatically export all variables
    source .env
    set +a # stop automatically exporting
fi

# This script is a simple wrapper to run Python scripts with UV
# Usage: ./run_with_uv.sh [python_options] <script_name.py> [args]
#   or   ./run_with_uv.sh -c 'python code'  (use single quotes to avoid issues with special characters)
#   or   ./run_with_uv.sh -m module_name

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: ./run_with_uv.sh [python_options] <script_name.py> [args]"
    echo "Examples:"
    echo "  ./run_with_uv.sh email_scheduler_optimized.py --input data.json --output results.json"
    echo "  ./run_with_uv.sh -c 'import sys; print(sys.version)'  # Note: Use single quotes for -c option"
    echo "  ./run_with_uv.sh -m unittest discover"
    exit 1
fi

# Special handling for -c option to work around history expansion issues
if [ "$1" = "-c" ]; then
    # Create a temporary file for the Python code
    TEMP_FILE=$(mktemp)
    # Write the Python code to the file, without shell expansion
    printf "%s\n" "$2" > "$TEMP_FILE"
    echo "Running Python code with UV..."
    uv run python -c "$(cat "$TEMP_FILE")"
    exit_code=$?
    # Clean up temp file
    rm -f "$TEMP_FILE"
elif [[ "$1" == "-m" || "$1" == "-V" || "$1" == "--version" ]]; then
    # Other Python interpreter options
    echo "Running Python with UV using option $1..."
    uv run python "$@"
    exit_code=$?
else
    # This is likely a script filename
    SCRIPT="$1"
    shift  # Remove the first argument
    
    echo "Running $SCRIPT with UV..."
    uv run python "$SCRIPT" "$@"
    exit_code=$?
fi

# Check if the script executed successfully
if [ $exit_code -eq 0 ]; then
    echo "Script executed successfully."
else
    echo "Script failed with exit code $exit_code."
fi

