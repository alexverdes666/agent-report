#!/bin/bash
# This file is intentionally left blank.
# The build process is now handled by the Dockerfile.
set -e
echo "Build is handled by Dockerfile. This script is not used."

echo "Starting Render build process..."

# Install Python dependencies first
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Install Playwright browsers with system dependencies
echo "Installing Playwright browsers and dependencies..."
python -m playwright install --with-deps chromium

echo "Build completed successfully!" 