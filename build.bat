@echo off
REM =====================================================
REM  NotebookLM Document Merger -- Build portable .exe
REM  Double-click this file to rebuild the executable.
REM
REM  Requirements:
REM    - Python 3.9+ installed and on PATH
REM    - All runtime dependencies installed
REM      (pip install -r requirements.txt)
REM
REM  Tip: If you get permission errors, try running as
REM       Administrator (right-click → Run as administrator)
REM =====================================================

echo.
echo Building NotebookLM_Merger.exe ...
echo.

python build_exe.py
if errorlevel 1 (
    echo.
    echo BUILD FAILED. See error output above.
    pause
    exit /b 1
)

echo.
echo Done. Executable is in the dist\ folder.
pause
