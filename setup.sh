#!/usr/bin/env bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "Setup complete. Run 'source venv/bin/activate' to activate."
