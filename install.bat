@echo off
echo Installing SQL MCP Server dependencies...

REM Check if Python is installed
python --version > nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed or not in PATH. Please install Python 3.8 or newer.
    exit /b 1
)

REM Check if virtual environment exists
if not exist .venv (
    echo Creating virtual environment...
    python -m venv .venv
) else (
    echo Virtual environment already exists.
)

REM Activate virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate.bat

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

REM Install dependencies
echo Installing dependencies...
pip install -r requirements.txt

REM Install the package in development mode
echo Installing SQL MCP Server in development mode...
pip install -e .

echo Installation complete.
echo.
echo To run the server:
echo 1. Update the .env file with your SQL credentials
echo 2. Run: .venv\Scripts\python main.py

pause
