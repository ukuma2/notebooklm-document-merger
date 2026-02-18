# NotebookLM Document Merger

🚀 **A user-friendly tool to merge large document collections into NotebookLM-compatible batches**

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## ✨ Features

### Smart Format Preservation
- **📄 PDFs** → Merged natively (preserves scanned images, no OCR needed!)
- **📝 DOCX** → Merged natively (preserves formatting, tables, images)
- **📧 Emails** → Intelligently threaded by conversation

### User-Friendly GUI
- Drag & drop folder selection
- Real-time progress tracking
- Works with any folder structure
- Configurable size limits

### NotebookLM Optimized
- Stays under 300-file upload limit
- Configurable batch sizes (default: 800KB)
- Generates tracking manifest

---

## 📥 Installation

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

## 🎮 Usage

### GUI Mode (Recommended)

1. **Launch the application**:
   ```bash
   python document_merger_gui.py
   ```

2. **Select your folders**:
   - Click "Browse..." to choose input folder (containing your documents)
   - Choose output folder (where merged files will be saved)

3. **Configure settings** (optional):
   - Select file types to process (PDFs, DOCX, Emails)
   - Adjust max file size and output limits

4. **Start merging**:
   - Click "🚀 Start Merging"
   - Monitor progress
   - Upload merged files to NotebookLM!

### Example

**Before**: 1000+ files in various formats  
**After**: <300 merged files ready for NotebookLM

```
Input/
├── Case1/
│   ├── contract.pdf
│   ├── email1.msg
│   └── report.docx
└── Case2/
    └── documents/

Output/
├── Case1_pdfs_batch1.pdf
├── Case1_documents_batch1.docx
├── Case1_emails_thread1.txt
└── merge_manifest.json
```

---

## 🔧 How It Works

### Intelligent Merging Strategy

1. **Folder Analysis**: Auto-detects folder structure and grouping
2. **File Categorization**: Separates PDFs, DOCX, and emails
3. **Smart Batching**: Monitors file sizes and creates batches under limits
4. **Format Preservation**: 
   - PDFs merged using pypdf (preserves everything including scanned images)
   - DOCX merged using python-docx (preserves formatting and embedded content)
   - Emails threaded by conversation and extracted to text

### Email Threading

Emails are intelligently grouped:
- Normalizes subjects (removes "RE:", "FW:", etc.)
- Groups by conversation
- Sorts chronologically
- Preserves all metadata

---

## 📁 Project Structure

```
notebooklm-document-merger/
├── document_merger_gui.py    # GUI application
├── merger_engine.py           # Core merging logic
├── requirements.txt           # Python dependencies
├── README.md                  # This file
└── .gitignore                 # Git ignore rules
```

---

## 🛠️ Advanced Options

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

## 🐛 Troubleshooting

**Q: GUI won't start**
- Ensure Python 3.8+ is installed: `python --version`
- Install dependencies: `pip install -r requirements.txt`

**Q: "Module not found" error**
- Run: `pip install -r requirements.txt`

**Q: PDF merging fails**
- Some encrypted PDFs may not merge
- Check console for specific error messages

**Q: DOCX formatting issues**
- Complex formatting may not transfer perfectly
- Basic styles, tables, and images are preserved

---

## 📊 Technical Details

### Dependencies
- **pypdf** (≥4.0.0): PDF merging
- **python-docx** (≥1.1.0): DOCX creation/merging
- **extract-msg** (≥0.45.0): Outlook MSG parsing
- **python-dateutil** (≥2.8.2): Date parsing

### System Requirements
- Python 3.8+
- Windows / macOS / Linux
- 100MB disk space
- 4GB RAM recommended

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Ideas for Enhancement
- [ ] OCR integration for scanned PDFs
- [ ] Custom grouping rules
- [ ] Preview before merging
- [ ] Batch rename utility
- [ ] Undo/rollback functionality

---

## 📝 License

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

## 🌟 Acknowledgments

Built for use with [Google NotebookLM](https://notebooklm.google.com/)

---

## 📧 Support

For issues or questions, please [open an issue](https://github.com/YOUR_USERNAME/notebooklm-document-merger/issues) on GitHub.

---

**Made with ❤️ for NotebookLM users**
