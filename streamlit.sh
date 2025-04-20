#!/bin/bash

echo "[TERMINAL] Activating virtual environment"
python3 -m venv venv
source venv/bin/activate

echo "[TERMINAL] Upgrading pip to avoid warnings"
pip install --upgrade pip

echo "[TERMINAL] Installing Python dependencies"
pip install -r resources/requirements.txt

echo "[TERMINAL] Running streamlit app.py"
streamlit run resources/streamlit_app.py

echo "[TERMINAL] Deactivating virtual environment"
deactivate