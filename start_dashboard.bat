@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ======================================================================
echo   DANG KHOI DONG WEB DASHBOARD DO AN BIG DATA (DATA PLATFORM CONSOLE)...
echo   Dia chi truy cap: http://localhost:8080
echo ======================================================================
py src/web_dashboard.py
pause

