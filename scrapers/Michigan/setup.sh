#!/bin/bash
# Setup script for Michigan Election Scraper

echo "Setting up Michigan Election Scraper..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install requirements
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "Setup complete!"
echo ""
echo "To run the scraper:"
echo "  1. Activate the virtual environment: source venv/bin/activate"
echo "  2. Run the scraper: python michigan_scraper.py"
echo ""

