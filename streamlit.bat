@echo off

echo [COMMAND PROMPT] Activating virtual environment
python -m venv venv
call venv\Scripts\activate

echo [COMMAND PROMPT] Upgrading pip to avoid warnings
pip install --upgrade pip

echo [COMMAND PROMPT] Installing Python dependencies
pip install -r resources\requirements.txt

echo [COMMAND PROMPT] Running streamlit app.py
streamlit run resources\streamlit_app.py

echo [COMMAND PROMPT] Deactivating virtual environment
deactivate