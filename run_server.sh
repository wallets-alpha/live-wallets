#!/bin/bash

# Define the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Check if a virtual environment exists, create if it doesn't
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install websocket-client pyee
else
    source venv/bin/activate
fi

# Create log directory if it doesn't exist
mkdir -p logs

# Set GitHub repository URL (defaults to the one in the script, but can be overridden)
export GITHUB_REPO_URL="https://github.com/wallets-alpha/live-wallets.git"

# Start the server in the background with nohup
echo "Starting wallet scraper server..."
nohup python server.py > logs/server.log 2>&1 &

# Save the process ID for potential later use
echo $! > server.pid

echo "Server started with PID $(cat server.pid)"
echo "Logs are being written to logs/server.log"
echo "To check logs, use: tail -f logs/server.log"
echo "To stop the server, use: kill $(cat server.pid)" 
