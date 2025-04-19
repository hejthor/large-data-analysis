#!/bin/bash

echo "[TERMINAL] Activating virtual environment"
python3 -m venv venv
source venv/bin/activate

echo "[TERMINAL] Upgrading pip to avoid warnings"
pip install --upgrade pip

echo "[TERMINAL] Installing Python dependencies"
pip install -r resources/requirements.txt

echo "[TERMINAL] Running generate.py"
python resources/generate.py --file_path output/data.csv --rows 1000000 --seed 42

echo "[TERMINAL] Deactivating virtual environment"
deactivate