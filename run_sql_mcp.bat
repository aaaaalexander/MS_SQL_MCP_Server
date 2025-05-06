@echo off
echo Starting SQL MCP Server...

REM Activate virtual environment if exists
call .venv\Scripts\activate.bat 2>nul

REM Load settings from .env file if it exists
echo Loading configuration from .env...
if exist .env (
    for /F "tokens=1,2 delims==" %%a in (.env) do (
        set %%a=%%b
    )
    echo Configuration loaded successfully
) else (
    echo WARNING: .env file not found. Using default values.
)

REM Set required environment variables
set PYTHONUNBUFFERED=1

REM Check if server and credentials are set
if "%DB_SERVER%"=="" (
    echo ERROR: DB_SERVER is not set. Please check your .env file.
    goto error
)
if "%DB_NAME%"=="" (
    echo WARNING: DB_NAME is not set. Using default value: database
    set DB_NAME=database
)
if "%DB_USERNAME%"=="" (
    echo ERROR: DB_USERNAME is not set. Please check your .env file.
    goto error
)
if "%DB_PASSWORD%"=="" (
    echo ERROR: DB_PASSWORD is not set. Please check your .env file.
    goto error
)
if "%DB_ALLOWED_SCHEMAS%"=="" (
    echo INFO: DB_ALLOWED_SCHEMAS is not set. Using default value: ["dbo"]
    set DB_ALLOWED_SCHEMAS=["dbo"]
)

REM Display current configuration
echo.
echo Current Configuration:
echo ---------------------
echo Server:    %DB_SERVER%
echo Database:  %DB_NAME%
echo Username:  %DB_USERNAME%
echo Password:  ***************
echo Schemas:   %DB_ALLOWED_SCHEMAS%
echo.

REM Ask for debug mode
set /p DEBUG_MODE="Enable debug mode with verbose logging? (Y/N) [N]: "
if /i "%DEBUG_MODE%"=="Y" (
    set DB_DEBUG=true
    set DB_LOG_LEVEL=DEBUG
    echo Debug mode enabled.
) else (
    set DB_DEBUG=false
    set DB_LOG_LEVEL=INFO
)

REM Start the MCP server
echo.
echo Starting SQL MCP server...
echo Logs will appear below. Press Ctrl+C to stop the server.
echo.
python sql_mcp_server.py
goto end

:error
echo.
echo Server startup failed due to missing configuration.
echo Please check your .env file and try again.
echo See README.md for more information on configuration.

:end
echo.
echo Server exited with code %ERRORLEVEL%
pause