@echo off
echo ==========================================
echo Bluesky Post Collector
echo ==========================================

cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not added to PATH.
    echo Please install Python 3 first.
    pause
    exit /b 1
)

echo Installing required package...
python -m pip install atproto requests beautifulsoup4

echo.
echo Running crawlers in required order...
echo.

echo Step 1: Running Conspiracy Theories crawler first...
python conspiracy_theories_crawl.py %1 %2 %3 %4

echo.
echo Step 2: Running Paranormal Activity crawler...
python paranormal_theories_crawl.py

echo.
echo Step 3: Running UFO Theories crawler...
python ufo_theories_crawl.py

echo.
echo Step 4: Running Strange Earth crawler...
python Strange_Earth_crawl.py

echo.
echo ==========================================
echo Collection complete.
echo All crawlers finished.
echo ==========================================
pause