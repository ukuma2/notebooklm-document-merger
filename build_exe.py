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
from pathlib import Path

ROOT = Path(__file__).parent
ICON = ROOT / "assets" / "icon.ico"
ENTRY = ROOT / "document_merger_gui.py"
APP_NAME = "NotebookLM_Merger"


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
        # Clean build artifacts before building
        "--clean",
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
