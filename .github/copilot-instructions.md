# Copilot Instructions for NotebookLM Document Merger

## Project Overview

This is a Python desktop application that merges large document collections into
NotebookLM-compatible batches. It supports PDFs, Word documents (.doc/.docx), and
Outlook emails (.msg/.eml), staying within NotebookLM's 300-file upload limit.

## Architecture

- **`merger_engine.py`** — Core logic: file discovery, ZIP extraction, PDF merging,
  email threading, Word-to-PDF conversion, manifest generation.
- **`document_merger_gui.py`** — Tkinter GUI: folder/ZIP selection, progress display,
  settings controls. Runs the engine in a background thread.
- **`tests/`** — pytest test suite covering individual subsystems and integration paths.
- **`requirements.txt`** — Runtime dependencies (pypdf, extract-msg, python-dateutil,
  Pillow, olefile, pywin32 on Windows).

## Tech Stack

- Python 3.8+
- Tkinter (GUI, stdlib)
- pypdf ≥ 4.0 (PDF merging)
- python-docx (DOCX creation)
- extract-msg ≥ 0.45 (Outlook MSG parsing)
- python-dateutil (date parsing)
- pywin32 / win32com (Windows-only Word automation)

## Development Workflow

### Install dependencies
```bash
pip install -r requirements.txt
pip install flake8 pylint pytest
```

### Lint
```bash
# Hard errors only (syntax errors, undefined names)
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
# Style warnings (non-blocking)
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
# Error/fatal pylint checks
pylint --exit-zero --disable=all --enable=E,F *.py
```

### Run tests
```bash
python -m pytest -v
```

Tests live in `tests/` and are named `*_test.py`. Shared fixtures are in
`tests/conftest.py` (e.g. `tmp_path`, `io_dirs`, `make_pdf`, `make_docx`,
`make_eml`, `patch_word_converter`).

## Coding Conventions

- Follow existing style: no type annotations beyond what is already present, use
  `Optional[...]` / `List[...]` / `Dict[...]` from `typing` (Python 3.8 compat).
- Max line length: 127 characters (matches flake8 config).
- Imports: stdlib first, then third-party, then local. No wildcard imports.
- Logging/warnings: use `_record_warning(warnings, code, message, **context)` in
  `merger_engine.py` rather than bare `print` statements.
- Guard optional dependencies with `try/except ImportError` and `HAS_*` booleans
  (see top of `merger_engine.py`).
- Keep GUI and engine fully separated: `document_merger_gui.py` imports from
  `merger_engine.py` but not the reverse.

## Output Structure

Every run produces four sibling folders inside the chosen output directory:
- `processed/` — merged outputs + `merge_manifest.json`
- `unprocessed/` — unsupported files extracted from ZIPs
- `failed/` — failure artifacts/metadata
- `logs/` — plain-text run log + JSONL event log

## Key Constraints

- NotebookLM upload limit: 300 files (configurable `max_output_files`).
- Default batch size: 100 MB / 102,400 KB per merged file.
- ZIP entry names are truncated to 50 characters (including extension) for
  Windows compatibility.
- Nested ZIPs are extracted one level deep only.
- If the output folder is inside the input folder, it is automatically excluded
  from scanning to avoid re-processing generated files.
