@echo off
echo Setting environment variables and running DB_USER server...

REM Set environment variables
set "DB_USER_DB_USERNAME=DB_USER"
set "DB_USER_DB_PASSWORD=YOUR_PASSWORD_HERE"
set "DB_USER_ALLOWED_SCHEMAS=dbo"
set "DB_USER_DEBUG=true"
set "DB_USER_LOG_LEVEL=DEBUG"

echo Environment variables set:
echo DB_USER_DB_USERNAME=%DB_USER_DB_USERNAME%
echo DB_USER_ALLOWED_SCHEMAS=%DB_USER_ALLOWED_SCHEMAS%
echo DB_USER_DEBUG=%DB_USER_DEBUG%
echo DB_USER_LOG_LEVEL=%DB_USER_LOG_LEVEL%

echo Running server...
python main.py
