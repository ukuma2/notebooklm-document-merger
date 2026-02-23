# NotebookLM Document Merger

ðŸš€ **A user-friendly tool to merge large document collections into NotebookLM-compatible batches**

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## âœ¨ Features

### Smart Format Preservation
- **ðŸ“„ PDFs** â†’ Merged natively (preserves scanned images, no OCR needed!)
- **ðŸ“ Word (.doc/.docx)** â†’ Converted with Microsoft Word to PDF, then merged as PDF batches
- **ðŸ“§ Emails** â†’ Subject-threaded and written into size-batched text outputs (default 25 MB)

### User-Friendly GUI
- Drag & drop folder selection
- Real-time progress tracking with live stage/event log
- Works with any folder structure
- Configurable size limits

### NotebookLM Optimized
- Stays under 300-file upload limit
- Configurable batch sizes (default: 100MB / 102400KB)
- Generates tracking manifest

---

## ðŸ“¥ Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)

### Quick Install

1. **Clone the repository**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/notebooklm-document-merger.git
   cd notebooklm-document-merger
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application**:
   ```bash
   python document_merger_gui.py
   ```

---

## ðŸŽ® Usage

### GUI Mode (Recommended)

1. **Launch the application**:
   ```bash
   python document_merger_gui.py
   ```

2. **Select your input/output**:
   - Use `Folder...` to choose an input folder, or `ZIP...` to choose a single ZIP archive
   - Choose output folder (where merged files will be saved)

3. **Configure settings** (optional):
   - Select file types to process (PDFs, Word docs, Emails)
   - Adjust max file size and output limits

4. **Start merging**:
   - Click "ðŸš€ Start Merging"
   - Monitor progress
   - Upload merged files to NotebookLM!

### Example

**Before**: 1000+ files in various formats  
**After**: <300 merged files ready for NotebookLM

```
Input/
â”œâ”€â”€ Case1/
â”‚   â”œâ”€â”€ contract.pdf
â”‚   â”œâ”€â”€ email1.msg
â”‚   â””â”€â”€ report.docx
â””â”€â”€ Case2/
    â””â”€â”€ documents/

Output/
â”œâ”€â”€ Case1_pdfs_batch1.pdf
â”œâ”€â”€ Case1_documents_batch1.pdf
â”œâ”€â”€ Case1_emails_batch1.txt
â””â”€â”€ merge_manifest.json
```

Output folders are always structured as:
- `processed/` (merged outputs + `merge_manifest.json`)
- `unprocessed/` (relocated unsupported files from ZIP + normal input)
- `failed/` (copied failed file artifacts + metadata)
- `logs/` (run text + JSONL logs)

---

## ðŸ”§ How It Works

### Intelligent Merging Strategy

1. **Folder Analysis**: Auto-detects folder structure and grouping
2. **ZIP Preprocessing**: Auto-extracts `.zip` archives with long-filename truncation for Windows compatibility
3. **File Categorization**: Separates processable and unsupported files
4. **Smart Batching**: Monitors file sizes and creates batches under limits
5. **Format Preservation**: 
   - PDFs merged using pypdf (preserves everything including scanned images)
   - Word files are converted with Microsoft Word to PDF, then merged via the PDF pipeline
   - Emails are grouped by subject thread, then packed into size-capped batch files with attachment indexes

### Email Threading

Emails are intelligently grouped:
- Normalizes subjects (removes "RE:", "FW:", etc.)
- Groups by conversation
- Sorts chronologically
- Preserves all metadata

### ZIP Archive Processing

- ZIP archives are processed automatically alongside normal files
- Entry names are truncated to 50 characters by default (including extension)
- Nested ZIP archives are extracted one level deep
- ZIP contents are grouped under a synthetic group name based on the ZIP filename (for example: `root_Emails-49431`)
- Unsupported files are relocated under `unprocessed/zip/<group>/...`
- Unsupported normal input files are relocated under `unprocessed/input/...`

---

## ðŸ“ Project Structure

```
notebooklm-document-merger/
â”œâ”€â”€ document_merger_gui.py    # GUI application
â”œâ”€â”€ merger_engine.py           # Core merging logic
â”œâ”€â”€ requirements.txt           # Python dependencies
â”œâ”€â”€ README.md                  # This file
â””â”€â”€ .gitignore                 # Git ignore rules
```

---

## ðŸ› ï¸ Advanced Options

### Creating Standalone Executable

To create a `.exe` file that doesn't require Python:

1. Install PyInstaller:
   ```bash
   pip install pyinstaller
   ```

2. Build executable:
   ```bash
   pyinstaller --name="NotebookLM_Merger" --onefile --windowed document_merger_gui.py
   ```

3. Find executable in `dist/` folder

---

## ðŸ› Troubleshooting

**Q: GUI won't start**
- Ensure Python 3.8+ is installed: `python --version`
- Install dependencies: `pip install -r requirements.txt`

**Q: "No module named 'tkinter'" error**
- **Ubuntu/Debian**: `sudo apt-get install python3-tk`
- **Fedora**: `sudo dnf install python3-tkinter`
- **macOS**: Tkinter should be included with Python. Reinstall Python from python.org if missing
- **Windows**: Tkinter is included by default. Reinstall Python and ensure "tcl/tk and IDLE" is checked

**Q: "Module not found" error**
- Run: `pip install -r requirements.txt`

**Q: PDF merging fails**
- Some encrypted PDFs may not merge
- Check console for specific error messages

**Q: Word document quality issues**
- Use Windows with Microsoft Word installed for highest-fidelity `.doc/.docx` conversion
- Failed conversions are skipped and recorded as warnings in `processed/merge_manifest.json`

**Q: Emoji characters not displaying properly**
- The GUI uses Unicode emojis (ðŸ“„, ðŸ“, ðŸ“§, ðŸš€) which may not render on older systems
- On Windows 7/8 or minimal Linux systems, you may see boxes instead of emojis
- This is a cosmetic issue and doesn't affect functionality

---

## ðŸ“Š Technical Details

### Dependencies
- **pypdf** (â‰¥4.0.0): PDF merging
- **pywin32** (Windows only): Microsoft Word automation for Word-to-PDF conversion
- **extract-msg** (â‰¥0.45.0): Outlook MSG parsing
- **python-dateutil** (â‰¥2.8.2): Date parsing

### ZIP Defaults
- ZIP preprocessing is enabled by default
- ZIP filename limit: 50 characters (including extension)
- Nested ZIP extraction depth: 1 level

### Email Output Defaults
- Email output mode defaults to `size_batched`
- Email batch size cap defaults to 25 MB per output file
- Email outputs include body text and attachment metadata index (no binary attachment payload embedding)

### Real-Data ZIP Smoke Check
Run this command to validate ZIP handling against your large archive outside CI:

```bash
python tests/smoke_zip_real_data.py --zip Emails-49431.zip --workdir ._tmp_zip_real_smoke
```

### System Requirements
- Python 3.8+
- Windows / macOS / Linux
- 100MB disk space
- 4GB RAM recommended

---

## ðŸ¤ Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Ideas for Enhancement
- [ ] OCR integration for scanned PDFs
- [ ] Custom grouping rules
- [ ] Preview before merging
- [ ] Batch rename utility
- [ ] Undo/rollback functionality

### ðŸ¤– Automated Workflows

This repository uses GitHub Actions to automate pull request validation and merging. Here's what happens automatically:

#### PR Validation
When you submit a pull request:
- **âœ… Automated Testing & Linting**: Code is automatically checked with flake8 and pylint
- **ðŸ” Conflict Detection**: Checks for merge conflicts with the base branch
- **ðŸ·ï¸ Auto-Labeling**: PRs are automatically labeled based on:
  - File types changed (python, documentation, dependencies, github-actions)
  - PR size (small <50 lines, medium <200 lines, large 200+ lines)
  - Source (external-contribution for forks)

#### Auto-Merge System
PRs can be automatically merged when ALL of these conditions are met:
- âœ… All required checks pass (linting, tests)
- âœ… No merge conflicts with base branch
- âœ… Approved by at least one reviewer **OR** has the `auto-merge` label
- âœ… Branch is up to date

**To enable auto-merge on your PR:**
1. Wait for a project maintainer to review and approve your PR, OR
2. If you're a trusted contributor, add the `auto-merge` label

**For Fork Contributors:**
- All workflows run safely on PRs from forks
- Auto-merge requires approval from a maintainer
- Your code is automatically checked without requiring manual intervention

#### ðŸ”’ Security Features

**For Fork Contributors:**
- All workflows run safely without access to repository secrets
- Code is checked out in read-only mode
- Auto-merge requires maintainer approval for all external PRs

**Dependabot Auto-Merge Policy:**
- âœ… **Patch updates** (1.2.3 â†’ 1.2.4): Auto-merged after checks pass
- âš ï¸ **Minor updates** (1.2.0 â†’ 1.3.0): Requires manual review
- ðŸš¨ **Major updates** (1.0.0 â†’ 2.0.0): Requires manual review

**Security Scanning:**
- CodeQL analysis runs on all PRs and weekly
- Dependency review checks for known vulnerabilities
- Moderate+ severity issues block PRs

#### Dependabot Updates
- Automatically checks for Python dependency updates weekly (Mondays at 9 AM)
- Creates PRs for security updates and version bumps
- Patch updates are auto-merged after passing checks
- Minor and major updates require manual review

#### Branch Protection Recommendations
For repository maintainers, recommended branch protection rules for `main`:
- Require pull request reviews (at least 1 approval)
- Require status checks to pass before merging
- Required checks: "Lint and Test", "Check for Merge Conflicts"
- Require branches to be up to date before merging
- Include administrators in restrictions

---

## Operational Known Limits

- Password-protected or heavily malformed PDFs may be skipped and reported in `processed/merge_manifest.json`.
- Word conversion requires Microsoft Word on Windows; files that fail conversion are skipped with warnings.
- Very large source sets can exceed `Max Output Files`; the run stops with a clear limit error before writing excess files.
- If output is placed inside input, the output folder is automatically excluded from scanning to prevent re-processing generated files.
- Email threading is subject-based and date-sorted; missing or invalid dates are treated as oldest items in a thread.

---

## ðŸ“ License

This project is licensed under the MIT License - see below for details.

```
MIT License

Copyright (c) 2026

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## ðŸŒŸ Acknowledgments

Built for use with [Google NotebookLM](https://notebooklm.google.com/)

---

## ðŸ“§ Support

For issues or questions, please [open an issue](https://github.com/YOUR_USERNAME/notebooklm-document-merger/issues) on GitHub.

---

**Made with â¤ï¸ for NotebookLM users**


