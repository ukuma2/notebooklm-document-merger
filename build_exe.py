"""
Build script for NotebookLM Document Merger portable Windows .exe

Usage:
    python build_exe.py

Output:
    dist/NotebookLM_Merger.exe   (portable, no installer needed)

Requirements:
    pip install pyinstaller>=6.0
"""

import subprocess
import sys
import os
import shutil
import time
from pathlib import Path

ROOT = Path(__file__).parent
ICON = ROOT / "assets" / "icon.ico"
ENTRY = ROOT / "document_merger_gui.py"
APP_NAME = "NotebookLM_Merger"


def cleanup_build_dirs():
    """Safely remove build and dist directories to prevent PyInstaller cleanup errors."""
    for dirname in ["build", "dist"]:
        dirpath = ROOT / dirname
        if dirpath.exists():
            try:
                print(f"Cleaning {dirname}/ directory...")
                shutil.rmtree(dirpath)
            except PermissionError:
                print(f"  WARNING: Could not fully remove {dirname}/ (may be locked)")
                print(f"  Attempting to continue anyway...")
            # Small delay to ensure OS releases file locks, even if deletion partially failed
            time.sleep(0.5)


def main():
    if not ENTRY.exists():
        print(f"ERROR: Entry point not found: {ENTRY}")
        sys.exit(1)

    # Try to import PyInstaller; prompt to install if missing.
    try:
        import PyInstaller  # noqa: F401
    except ImportError:
        print("PyInstaller not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller>=6.0"])

    # Clean up build directories before PyInstaller runs
    cleanup_build_dirs()

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--windowed",           # no console window
        f"--name={APP_NAME}",
        # Hidden imports needed because PyInstaller can't always detect these
        "--hidden-import=pypdf",
        "--hidden-import=PIL",
        "--hidden-import=PIL._tkinter_finder",
        "--hidden-import=docx",
        "--hidden-import=extract_msg",
        "--hidden-import=dateutil",
        "--hidden-import=dateutil.parser",
        "--hidden-import=olefile",
        # pywin32 COM automation for Word conversion
        "--hidden-import=win32com",
        "--hidden-import=win32com.client",
        "--hidden-import=pythoncom",
        "--hidden-import=pywintypes",
        # extract_msg uses pkg_resources / importlib.metadata internally
        "--collect-all=extract_msg",
        str(ENTRY),
    ]

    # Add icon if it exists
    if ICON.exists():
        cmd.insert(cmd.index("--clean"), f"--icon={ICON}")
    else:
        print(f"INFO: No icon found at {ICON} — building without icon.")
        print("      To add a firm icon, place a .ico file at assets/icon.ico and rebuild.")

    print("\n" + "=" * 60)
    print(f"Building {APP_NAME}.exe ...")
    print("=" * 60)
    print(" ".join(str(c) for c in cmd))
    print()

    result = subprocess.run(cmd, cwd=ROOT)

    if result.returncode != 0:
        print("\nERROR: PyInstaller build failed (see output above).")
        sys.exit(result.returncode)

    exe_path = ROOT / "dist" / f"{APP_NAME}.exe"
    print("\n" + "=" * 60)
    if exe_path.exists():
        size_mb = exe_path.stat().st_size / (1024 * 1024)
        print(f"SUCCESS: {exe_path}  ({size_mb:.1f} MB)")
        print("Share this file with users — no Python installation needed.")
    else:
        print(f"WARNING: Build finished but {exe_path} not found. Check PyInstaller output.")
    print("=" * 60)


if __name__ == "__main__":
    main()
