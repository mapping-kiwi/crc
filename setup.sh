#!/bin/bash
echo "Setting up Manitoba Wildfire Evacuation Pipeline..."

# Check Python version
python3 --version

# Install dependencies
pip install -r requirements.txt

echo "Setup complete! Run 'python3 pipeline.py' to start."
