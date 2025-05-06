@echo off
echo Setting up SQL MCP Server...

REM Create virtual environment if it doesn't exist
if not exist .venv (
  echo Creating virtual environment...
  python -m venv .venv
)

REM Activate virtual environment
call .venv\Scripts\activate.bat

REM Install requirements
echo Installing required packages...
pip install -r requirements.txt

REM Install ODBC driver if not already installed
echo.
echo IMPORTANT: Make sure the Microsoft ODBC Driver for SQL Server is installed.
echo If not installed, please download and install it from:
echo https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
echo.

REM Create .env file if it doesn't exist
if not exist .env (
  echo Creating .env file...
  echo DB_USER_DB_SERVER=localhost > .env
  echo DB_USER_DB_NAME=database >> .env
  echo DB_USER_DB_USERNAME=your_username_here >> .env
  echo DB_USER_DB_PASSWORD=your_password_here >> .env
  echo DB_USER_ALLOWED_SCHEMAS=["dbo"] >> .env
  echo.
  echo IMPORTANT: Please edit the .env file to set your SQL Server credentials.
)

echo Setup complete!
echo.
echo Next steps:
echo 1. Edit the .env file with your SQL Server credentials
echo 2. Update the claude_config.json file to configure Claude Desktop
echo 3. Run the SQL MCP server using run_sql_mcp.bat

pause