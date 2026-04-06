@echo off
title Azure SQL Editor (Web)

echo Installing dependencies...
pip install -r requirements.txt --quiet

echo.
echo Starting Azure SQL Editor at http://localhost:8000
echo Press Ctrl+C to stop.
echo.
python app.py

if errorlevel 1 (
    echo.
    echo Application exited with an error.
    pause
)
