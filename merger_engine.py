"""
Document Merger Engine - Core merging logic
Handles PDF, DOCX, and Email merging with flexible folder structure support
"""

import os
import json
from copy import deepcopy
from datetime import datetime, timezone
import shutil
import tempfile
import uuid
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import re

# PDF handling
try:
    from pypdf import PdfReader, PdfWriter
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

# Image handling (for converting images masquerading as PDFs)
try:
    from PIL import Image
    import io
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# DOCX handling
try:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
    HAS_DOCX = True
except ImportError:
    HAS_DOCX = False

# Email handling
try:
    import extract_msg
    HAS_EXTRACT_MSG = True
except ImportError:
    HAS_EXTRACT_MSG = False

# Microsoft Word automation (Windows)
try:
    import pythoncom
    import win32com.client as win32_client
    HAS_WIN32COM = True
except ImportError:
    HAS_WIN32COM = False

from email import policy
from email.parser import BytesParser
from dateutil import parser as date_parser


def _record_warning(warnings: Optional[List[Dict]], code: str, message: str, **context) -> None:
    """Append a structured warning when a warning collector is provided."""
    if warnings is None:
        return
    warning = {'code': code, 'message': message}
    warning.update(context)
    warnings.append(warning)


def _make_writable_temp_dir(prefix: str) -> str:
    """
    Create a writable temporary directory.
    Some Windows/Python builds can produce temp dirs that are not writable when
    created with tempfile.mkdtemp(mode=0o700 semantics).
    """
    base_candidates = [tempfile.gettempdir(), os.getcwd()]

    for base_dir in base_candidates:
        if not base_dir:
            continue
        try:
            os.makedirs(base_dir, exist_ok=True)
        except OSError:
            continue

        for _ in range(8):
            candidate = os.path.join(base_dir, f"{prefix}{uuid.uuid4().hex}")
            try:
                os.makedirs(candidate, exist_ok=False)
                probe = os.path.join(candidate, ".write_probe")
                with open(probe, "wb") as handle:
                    handle.write(b"ok")
                os.remove(probe)
                return candidate
            except OSError:
                shutil.rmtree(candidate, ignore_errors=True)

    raise RuntimeError("Unable to create a writable temporary directory.")


class WordToPdfConverter:
    """Converts .doc/.docx files to PDF using Microsoft Word COM automation."""

    def __init__(self, warnings: Optional[List[Dict]] = None):
        self.warnings = warnings
        self.word_app = None
        self.com_initialized = False

    @staticmethod
    def is_available() -> Tuple[bool, str]:
        if os.name != 'nt':
            return False, "Word conversion is supported on Windows only."
        if not HAS_WIN32COM:
            return False, "pywin32 is required for Word-to-PDF conversion."
        return True, ""

    def __enter__(self):
        pythoncom.CoInitialize()
        self.com_initialized = True
        self.word_app = win32_client.DispatchEx("Word.Application")
        self.word_app.Visible = False
        self.word_app.DisplayAlerts = 0
        try:
            # 3 == msoAutomationSecurityForceDisable
            self.word_app.AutomationSecurity = 3
        except Exception:
            pass
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.word_app is not None:
            try:
                self.word_app.Quit()
            except Exception:
                pass
            self.word_app = None
        if self.com_initialized:
            pythoncom.CoUninitialize()
            self.com_initialized = False
        return False

    def convert_file(self, source_path: str, output_pdf_path: str) -> bool:
        """Convert a single Word document to PDF. Returns True on success."""
        if self.word_app is None:
            raise RuntimeError("Word automation session is not initialized.")

        document = None
        try:
            os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)
            input_abs = os.path.abspath(source_path)
            output_abs = os.path.abspath(output_pdf_path)

            document = self.word_app.Documents.Open(
                input_abs,
                ReadOnly=True,
                AddToRecentFiles=False,
                Visible=False,
                ConfirmConversions=False,
            )

            # wdExportFormatPDF = 17
            if hasattr(document, "ExportAsFixedFormat"):
                document.ExportAsFixedFormat(output_abs, 17)
            else:
                # Fallback for older Office object models.
                document.SaveAs(output_abs, FileFormat=17)

            return os.path.exists(output_abs) and os.path.getsize(output_abs) > 0
        except Exception as exc:
            _record_warning(
                self.warnings,
                'word_to_pdf_failed',
                'Word-to-PDF conversion failed; skipping file',
                file=source_path,
                error=str(exc),
            )
            return False
        finally:
            if document is not None:
                try:
                    document.Close(SaveChanges=False)
                except Exception:
                    pass


class PDFMerger:
    """Merges multiple PDF files into batched output files"""
    
    def __init__(self, max_file_size_kb=102400):
        self.max_file_size_kb = max_file_size_kb
        self.max_file_size_bytes = max_file_size_kb * 1024
        
    def estimate_batch_count(self, pdf_files: List[str]) -> int:
        """Estimate how many output batches a merge operation will create."""
        pdf_files = sorted(pdf_files)
        if not pdf_files:
            return 0

        batches = 1
        current_batch_size = 0

        for pdf_file in pdf_files:
            try:
                file_size = os.path.getsize(pdf_file)
            except OSError:
                # Avoid underestimating in preflight checks.
                file_size = self.max_file_size_bytes

            if current_batch_size and (current_batch_size + file_size > self.max_file_size_bytes):
                batches += 1
                current_batch_size = 0

            current_batch_size += file_size

        return batches

    def merge_pdfs(
        self,
        pdf_files: List[str],
        output_path: str,
        group_name: str,
        warnings: Optional[List[Dict]] = None,
        output_label: str = "pdfs",
        bookmark_titles: Optional[Dict[str, str]] = None,
        source_file_map: Optional[Dict[str, str]] = None,
        output_to_sources: Optional[Dict[str, List[str]]] = None,
    ) -> List[str]:
        """
        Merge PDF files into batches, staying under size limit
        
        Args:
            pdf_files: List of PDF file paths to merge
            output_path: Directory to save merged PDFs
            group_name: Name prefix for output files (e.g., "case_12345")
            
        Returns:
            List of created output file paths
        """
        if not HAS_PYPDF:
            raise ImportError("pypdf library is required for PDF merging")
        
        os.makedirs(output_path, exist_ok=True)
        output_files = []
        
        # Sort PDFs by name for consistent ordering
        pdf_files = sorted(pdf_files)
        
        current_batch = []
        current_batch_size = 0
        batch_num = 1
        
        for pdf_file in pdf_files:
            try:
                file_size = os.path.getsize(pdf_file)
            except OSError as exc:
                _record_warning(
                    warnings,
                    'pdf_stat_failed',
                    'Could not determine PDF file size; using max batch size for pre-allocation',
                    file=pdf_file,
                    error=str(exc),
                )
                file_size = self.max_file_size_bytes
            
            # If adding this file would exceed limit, save current batch
            if current_batch and (current_batch_size + file_size > self.max_file_size_bytes):
                output_file = self._save_pdf_batch(
                    current_batch,
                    output_path,
                    group_name,
                    batch_num,
                    warnings,
                    output_label=output_label,
                    bookmark_titles=bookmark_titles,
                    source_file_map=source_file_map,
                    output_to_sources=output_to_sources,
                )
                if output_file:
                    output_files.append(output_file)
                batch_num += 1
                current_batch = []
                current_batch_size = 0
            
            current_batch.append(pdf_file)
            current_batch_size += file_size
        
        # Save remaining files
        if current_batch:
            output_file = self._save_pdf_batch(
                current_batch,
                output_path,
                group_name,
                batch_num,
                warnings,
                output_label=output_label,
                bookmark_titles=bookmark_titles,
                source_file_map=source_file_map,
                output_to_sources=output_to_sources,
            )
            if output_file:
                output_files.append(output_file)
        
        return output_files
    
    def _try_convert_image_to_pdf(self, file_path: str) -> Optional[bytes]:
        """
        Attempt to open a file as an image and convert it to PDF bytes.
        Returns PDF bytes on success, or None if the file is not a valid image.
        """
        if not HAS_PIL:
            return None
        try:
            img = Image.open(file_path)
            # Convert to RGB so it can be saved as PDF (handles RGBA, P, etc.)
            if img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')
            pdf_bytes = io.BytesIO()
            img.save(pdf_bytes, format='PDF', resolution=150)
            print(f"    Converted image to PDF: {os.path.basename(file_path)}")
            return pdf_bytes.getvalue()
        except Exception:
            return None

    def _try_convert_ole_doc_to_pdf(self, file_path: str) -> Optional[bytes]:
        """
        Attempt to extract text from an OLE (legacy .doc) file and render it
        as a simple PDF page. Returns PDF bytes on success, or None on failure.
        """
        if not HAS_PIL:
            return None
        try:
            import olefile
        except ImportError:
            return None

        try:
            ole = olefile.OleFileIO(file_path)
            # Extract text from the WordDocument stream
            text = ""
            if ole.exists('WordDocument'):
                # Try to get text from the Word Document stream
                # The actual text in .doc files is in a complex binary format,
                # but we can try extracting readable ASCII/Unicode content
                raw = ole.openstream('WordDocument').read()
                # Extract printable text chunks
                text_chunks = []
                current_chunk = []
                for byte in raw:
                    if 32 <= byte < 127 or byte in (10, 13, 9):
                        current_chunk.append(chr(byte))
                    else:
                        if len(current_chunk) > 3:  # Only keep chunks > 3 chars
                            text_chunks.append(''.join(current_chunk))
                        current_chunk = []
                if len(current_chunk) > 3:
                    text_chunks.append(''.join(current_chunk))
                text = '\n'.join(text_chunks)
            ole.close()

            if not text.strip():
                return None

            # Render text as an image then convert to PDF
            from PIL import ImageDraw, ImageFont
            # Create a page-sized image (A4 at 72dpi equivalent)
            page_width, page_height = 612, 792
            margin = 40
            line_height = 14
            max_chars_per_line = 80
            max_lines = (page_height - 2 * margin) // line_height

            # Word-wrap the text
            import textwrap
            lines = []
            for paragraph in text.split('\n'):
                wrapped = textwrap.wrap(paragraph, width=max_chars_per_line) or ['']
                lines.extend(wrapped)

            # Create pages
            pages = []
            for page_start in range(0, len(lines), max_lines):
                page_lines = lines[page_start:page_start + max_lines]
                img = Image.new('RGB', (page_width, page_height), 'white')
                draw = ImageDraw.Draw(img)
                # Add header
                basename = os.path.basename(file_path)
                draw.text((margin, margin // 2), f"[Extracted from: {basename}]", fill='gray')
                for i, line in enumerate(page_lines):
                    y = margin + i * line_height
                    draw.text((margin, y), line, fill='black')
                pages.append(img)

            if not pages:
                return None

            # Save all pages as a multi-page PDF
            pdf_bytes = io.BytesIO()
            if len(pages) == 1:
                pages[0].save(pdf_bytes, format='PDF', resolution=72)
            else:
                pages[0].save(pdf_bytes, format='PDF', resolution=72,
                              save_all=True, append_images=pages[1:])
            print(f"    Converted OLE doc to PDF: {os.path.basename(file_path)}")
            return pdf_bytes.getvalue()
        except Exception:
            return None


    def _save_pdf_batch(
        self,
        pdf_files: List[str],
        output_path: str,
        group_name: str,
        batch_num: int,
        warnings: Optional[List[Dict]] = None,
        output_label: str = "pdfs",
        bookmark_titles: Optional[Dict[str, str]] = None,
        source_file_map: Optional[Dict[str, str]] = None,
        output_to_sources: Optional[Dict[str, List[str]]] = None,
    ) -> Optional[str]:
        """Save a batch of PDFs into a single merged PDF"""
        writer = PdfWriter()
        total_pages_added = 0
        merged_batch_sources: List[str] = []

        def add_bookmark(title: str, page_index: int) -> None:
            if page_index < 0:
                return
            try:
                writer.add_outline_item(title, page_index)
            except Exception:
                try:
                    writer.addBookmark(title, page_index)
                except Exception:
                    pass
        
        # Add all pages from all PDFs
        for pdf_file in pdf_files:
            try:
                reader = PdfReader(pdf_file)
                page_start = total_pages_added
                file_pages_added = 0
                for page in reader.pages:
                    writer.add_page(page)
                    file_pages_added += 1
                if file_pages_added == 0:
                    _record_warning(
                        warnings,
                        'pdf_no_pages',
                        'PDF contained zero readable pages',
                        file=pdf_file,
                    )
                else:
                    if bookmark_titles and bookmark_titles.get(pdf_file):
                        add_bookmark(bookmark_titles[pdf_file], page_start)
                    merged_batch_sources.append(pdf_file)
                total_pages_added += file_pages_added
            except Exception as e:
                # Fallback 1: File may be an image with a .pdf extension
                pdf_bytes = self._try_convert_image_to_pdf(pdf_file)
                if not pdf_bytes:
                    # Fallback 2: File may be an OLE .doc with a .pdf extension
                    pdf_bytes = self._try_convert_ole_doc_to_pdf(pdf_file)
                if pdf_bytes:
                    try:
                        reader = PdfReader(io.BytesIO(pdf_bytes))
                        page_start = total_pages_added
                        converted_pages = 0
                        for page in reader.pages:
                            writer.add_page(page)
                            converted_pages += 1
                        total_pages_added += converted_pages
                        if converted_pages == 0:
                            _record_warning(
                                warnings,
                                'pdf_conversion_empty',
                                'Fallback conversion produced zero pages',
                                file=pdf_file,
                            )
                        else:
                            if bookmark_titles and bookmark_titles.get(pdf_file):
                                add_bookmark(bookmark_titles[pdf_file], page_start)
                            merged_batch_sources.append(pdf_file)
                    except Exception as e2:
                        _record_warning(
                            warnings,
                            'pdf_conversion_failed',
                            'Could not merge file after fallback conversion',
                            file=pdf_file,
                            error=str(e2),
                        )
                        print(f"Warning: Could not merge {pdf_file} even after conversion: {e2}")
                else:
                    _record_warning(
                        warnings,
                        'pdf_unreadable',
                        'Could not read PDF file and fallback conversion failed',
                        file=pdf_file,
                        error=str(e),
                    )
                    print(f"Warning: Could not merge {pdf_file}: {e}")

        if total_pages_added == 0:
            _record_warning(
                warnings,
                'pdf_empty_batch',
                'Skipped PDF batch because no readable pages were found',
                group=group_name,
                batch=batch_num,
                file_count=len(pdf_files),
            )
            print(f"Warning: Skipping empty PDF batch {batch_num} for group {group_name}")
            return None
        
        # Generate output filename
        output_filename = f"{group_name}_{output_label}_batch{batch_num}.pdf"
        output_file = os.path.join(output_path, output_filename)

        if output_to_sources is not None:
            mapped_sources: List[str] = []
            for merged_source in merged_batch_sources:
                original = source_file_map.get(merged_source, merged_source) if source_file_map else merged_source
                if original not in mapped_sources:
                    mapped_sources.append(original)
            output_to_sources[output_file] = mapped_sources
        
        # Write merged PDF
        with open(output_file, 'wb') as f:
            writer.write(f)
        
        print(f"    Created: {output_filename} ({len(pdf_files)} PDFs, {total_pages_added} pages)")
        return output_file


class DOCXMerger:
    """Merges multiple DOCX files into batched output files"""
    
    def __init__(self, max_file_size_kb=102400):
        self.max_file_size_kb = max_file_size_kb
        self.max_file_size_bytes = max_file_size_kb * 1024
        
    def estimate_batch_count(self, docx_files: List[str]) -> int:
        """Estimate how many output batches a DOCX merge operation will create."""
        docx_files = sorted(docx_files)
        if not docx_files:
            return 0

        batches = 1
        current_batch_size = 0

        for docx_file in docx_files:
            try:
                file_size = os.path.getsize(docx_file)
            except OSError:
                file_size = self.max_file_size_bytes

            if current_batch_size and (current_batch_size + file_size > self.max_file_size_bytes):
                batches += 1
                current_batch_size = 0

            current_batch_size += file_size

        return batches

    def merge_docx(
        self,
        docx_files: List[str],
        output_path: str,
        group_name: str,
        warnings: Optional[List[Dict]] = None,
    ) -> List[str]:
        """
        Merge DOCX files into batches, staying under size limit
        
        Args:
            docx_files: List of DOCX file paths to merge
            output_path: Directory to save merged DOCX files
            group_name: Name prefix for output files
            
        Returns:
            List of created output file paths
        """
        if not HAS_DOCX:
            raise ImportError("python-docx library is required for DOCX merging")
        
        os.makedirs(output_path, exist_ok=True)
        output_files = []
        
        # Sort files by name
        docx_files = sorted(docx_files)
        
        current_batch = []
        current_batch_size = 0
        batch_num = 1
        
        for docx_file in docx_files:
            try:
                file_size = os.path.getsize(docx_file)
            except OSError as exc:
                _record_warning(
                    warnings,
                    'docx_stat_failed',
                    'Could not determine document file size; using max batch size for pre-allocation',
                    file=docx_file,
                    error=str(exc),
                )
                file_size = self.max_file_size_bytes
            
            # Check if we need to start a new batch
            if current_batch and (current_batch_size + file_size > self.max_file_size_bytes):
                output_file = self._save_docx_batch(
                    current_batch, output_path, group_name, batch_num, warnings
                )
                if output_file:
                    output_files.append(output_file)
                batch_num += 1
                current_batch = []
                current_batch_size = 0
            
            current_batch.append(docx_file)
            current_batch_size += file_size
        
        # Save remaining files
        if current_batch:
            output_file = self._save_docx_batch(
                current_batch, output_path, group_name, batch_num, warnings
            )
            if output_file:
                output_files.append(output_file)
        
        return output_files
    
    def _try_extract_docx_text(self, file_path: str) -> Optional[str]:
        """
        Fallback: extract raw paragraph text from word/document.xml inside the zip.
        Works even when python-docx can't open the file due to broken relationships.
        """
        import zipfile
        import xml.etree.ElementTree as ET

        try:
            with zipfile.ZipFile(file_path, 'r') as z:
                # Find word/document.xml (may be at a different path in some files)
                candidates = [n for n in z.namelist() if n.endswith('document.xml')]
                if not candidates:
                    return None
                xml_bytes = z.read(candidates[0])
            root = ET.fromstring(xml_bytes)
            paragraphs = []
            for para in root.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'):
                texts = [t.text or '' for t in para.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t')]
                line = ''.join(texts)
                paragraphs.append(line)
            return '\n'.join(paragraphs) if paragraphs else None
        except Exception:
            return None

    def _try_extract_ole_text(self, file_path: str) -> Optional[str]:
        """
        Fallback: extract text from OLE (legacy .doc) compound files.
        These may appear as OOXML theme-only zips but contain actual content
        in OLE streams like WordDocument and 1Table.
        """
        try:
            import olefile
        except ImportError:
            return None
        try:
            if not olefile.isOleFile(file_path):
                return None
            ole = olefile.OleFileIO(file_path)
            text = ""
            if ole.exists('WordDocument'):
                raw = ole.openstream('WordDocument').read()
                text_chunks = []
                current_chunk = []
                for byte in raw:
                    if 32 <= byte < 127 or byte in (10, 13, 9):
                        current_chunk.append(chr(byte))
                    else:
                        if len(current_chunk) > 3:
                            text_chunks.append(''.join(current_chunk))
                        current_chunk = []
                if len(current_chunk) > 3:
                    text_chunks.append(''.join(current_chunk))
                text = '\n'.join(text_chunks)
            ole.close()
            return text.strip() if text.strip() else None
        except Exception:
            return None

    def _save_docx_batch(
        self,
        docx_files: List[str],
        output_path: str,
        group_name: str,
        batch_num: int,
        warnings: Optional[List[Dict]] = None,
    ) -> Optional[str]:
        """Save a batch of DOCX files into a single merged document."""
        import tempfile
        import shutil

        merged_doc = Document()
        merged_docs_count = 0

        for docx_file in docx_files:
            source_doc = None
            raw_text = None
            open_error = None

            # Attempt 1: open normally.
            try:
                source_doc = Document(docx_file)
            except Exception as e1:
                open_error = e1

                # Attempt 2: copy to a temp .docx and retry.
                # Handles .doc files that are actually OOXML with wrong extension.
                tmp_path = None
                try:
                    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.docx')
                    os.close(tmp_fd)
                    shutil.copy2(docx_file, tmp_path)
                    source_doc = Document(tmp_path)
                    print(f"    Recovered (as .docx): {os.path.basename(docx_file)}")
                except Exception:
                    source_doc = None
                finally:
                    if tmp_path and os.path.exists(tmp_path):
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass

                if source_doc is None:
                    # Attempt 3: extract raw text from the zip's document.xml.
                    raw_text = self._try_extract_docx_text(docx_file)
                    if raw_text:
                        print(f"    Recovered (raw text): {os.path.basename(docx_file)}")
                    else:
                        # Attempt 4: extract text from OLE compound document.
                        raw_text = self._try_extract_ole_text(docx_file)
                        if raw_text:
                            print(f"    Recovered (OLE text): {os.path.basename(docx_file)}")

            if source_doc is None and not raw_text:
                _record_warning(
                    warnings,
                    'docx_unreadable',
                    'Could not read DOCX/DOC file; skipping file',
                    file=docx_file,
                    error=str(open_error) if open_error else 'unknown_error',
                )
                print(f"Warning: Could not merge {docx_file}: {open_error}")
                continue

            if raw_text:
                if merged_docs_count > 0:
                    merged_doc.add_page_break()
                heading = merged_doc.add_heading(level=1)
                heading.text = f"Document: {os.path.basename(docx_file)}"
                merged_doc.add_paragraph(raw_text)
                merged_docs_count += 1
                continue

            try:
                elements = [
                    deepcopy(element)
                    for element in source_doc.element.body.iterchildren()
                    if not element.tag.endswith('}sectPr')
                ]
                if not elements:
                    _record_warning(
                        warnings,
                        'docx_empty_document',
                        'DOCX file had no readable body elements; skipping file',
                        file=docx_file,
                    )
                    continue

                if merged_docs_count > 0:
                    merged_doc.add_page_break()
                heading = merged_doc.add_heading(level=1)
                heading.text = f"Document: {os.path.basename(docx_file)}"

                for element in elements:
                    merged_doc.element.body.append(element)
                merged_docs_count += 1
            except Exception as e:
                _record_warning(
                    warnings,
                    'docx_append_failed',
                    'Could not append DOCX body elements; skipping file',
                    file=docx_file,
                    error=str(e),
                )
                print(f"Warning: Could not append content from {docx_file}: {e}")

        if merged_docs_count == 0:
            _record_warning(
                warnings,
                'docx_empty_batch',
                'Skipped DOCX batch because no readable documents were found',
                group=group_name,
                batch=batch_num,
                file_count=len(docx_files),
            )
            print(f"Warning: Skipping empty DOCX batch {batch_num} for group {group_name}")
            return None

        # Generate output filename
        output_filename = f"{group_name}_documents_batch{batch_num}.docx"
        output_file = os.path.join(output_path, output_filename)

        # Save merged document
        merged_doc.save(output_file)

        print(f"    Created: {output_filename} ({merged_docs_count} documents)")
        return output_file


class EmailExtractor:
    """Extracts email content and metadata from .msg and .eml files"""
    
    @staticmethod
    def extract_msg(file_path: str) -> Optional[Dict]:
        """Extract email data from .msg file"""
        if not HAS_EXTRACT_MSG:
            return None
            
        try:
            msg = extract_msg.Message(file_path)
            return {
                'subject': msg.subject or '(No Subject)',
                'from': msg.sender or '',
                'to': msg.to or '',
                'cc': msg.cc or '',
                'date': msg.date,
                'body': msg.body or ''
            }
        except Exception as e:
            print(f"Error extracting .msg file {file_path}: {e}")
            return None
    
    @staticmethod
    def extract_eml(file_path: str) -> Optional[Dict]:
        """Extract email data from .eml file"""
        try:
            with open(file_path, 'rb') as f:
                msg = BytesParser(policy=policy.default).parse(f)
            
            return {
                'subject': msg.get('subject', '(No Subject)'),
                'from': msg.get('from', ''),
                'to': msg.get('to', ''),
                'cc': msg.get('cc', ''),
                'date': msg.get('date'),
                'body': msg.get_body(preferencelist=('plain', 'html')).get_content() if msg.get_body() else ''
            }
        except Exception as e:
            print(f"Error extracting .eml file {file_path}: {e}")
            return None


class EmailThreader:
    """Groups emails into conversation threads"""
    
    @staticmethod
    def normalize_subject(subject: str) -> str:
        """Normalize email subject by removing RE:, FW:, etc."""
        if not subject:
            return ""
        
        # Remove prefixes like RE:, FW:, FWD:, etc.
        subject = re.sub(r'^(RE|FW|FWD):\s*', '', subject, flags=re.IGNORECASE)
        subject = re.sub(r'\s+', ' ', subject).strip()
        return subject.lower()

    @staticmethod
    def normalize_date(date_value) -> datetime:
        """Normalize date values to naive UTC datetimes for safe sorting."""
        if isinstance(date_value, datetime):
            parsed = date_value
        elif isinstance(date_value, str) and date_value.strip():
            try:
                parsed = date_parser.parse(date_value)
            except (ValueError, TypeError, OverflowError):
                return datetime.min
        else:
            return datetime.min

        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    
    def group_emails(self, email_data: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group emails into threads by normalized subject
        
        Args:
            email_data: List of dicts with 'subject', 'date', 'file_path', etc.
            
        Returns:
            Dict mapping thread_id to list of email dicts
        """
        threads = defaultdict(list)
        
        for email in email_data:
            normalized_subject = self.normalize_subject(email.get('subject', ''))
            thread_key = normalized_subject or f"no_subject_{email.get('file_path', '')}"
            threads[thread_key].append(email)
        
        # Sort emails within each thread by date
        for thread_emails in threads.values():
            thread_emails.sort(key=lambda e: self.normalize_date(e.get('date')))
        
        return dict(threads)


class FolderAnalyzer:
    """Analyzes folder structure and determines grouping strategy"""
    
    @staticmethod
    def analyze_structure(
        root_path: str,
        exclude_paths: Optional[List[str]] = None,
    ) -> Dict[str, List[str]]:
        """
        Analyze folder structure and group files by parent folder
        
        Args:
            root_path: Root directory to analyze
            exclude_paths: Optional list of directories to exclude from traversal
            
        Returns:
            Dict mapping group_name to list of file paths
        """
        groups = defaultdict(list)
        excluded = [
            os.path.normcase(os.path.abspath(path))
            for path in (exclude_paths or [])
            if path
        ]

        def is_excluded(candidate_path: str) -> bool:
            candidate = os.path.normcase(os.path.abspath(candidate_path))
            for excluded_path in excluded:
                if candidate == excluded_path or candidate.startswith(excluded_path + os.sep):
                    return True
            return False

        for dirpath, dirnames, filenames in os.walk(root_path):
            if is_excluded(dirpath):
                dirnames[:] = []
                continue

            dirnames[:] = [
                dirname
                for dirname in dirnames
                if not is_excluded(os.path.join(dirpath, dirname))
            ]

            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                
                # Determine group name from folder structure
                rel_path = os.path.relpath(dirpath, root_path)
                
                if rel_path == '.':
                    # Files in root directory
                    group_name = 'root'
                else:
                    # Use first subfolder as group name
                    group_name = rel_path.split(os.sep)[0]
                
                groups[group_name].append(file_path)
        
        return dict(groups)


class MergeOrchestrator:
    """Coordinates the entire merging process"""
    
    def __init__(self, max_file_size_kb=102400, max_output_files=300, 
                 process_pdfs=True, process_docx=True, process_emails=True):
        self.max_file_size_kb = max_file_size_kb
        self.pdf_merger = PDFMerger(max_file_size_kb)
        self.email_extractor = EmailExtractor()
        self.email_threader = EmailThreader()
        self.folder_analyzer = FolderAnalyzer()
        self.word_converter_factory = WordToPdfConverter
        self.max_output_files = max_output_files
        self.process_pdfs = process_pdfs
        self.process_docx = process_docx
        self.process_emails = process_emails
        
    def merge_documents(self, input_path: str, output_path: str, progress_callback=None) -> Dict:
        """
        Main entry point for document merging
        
        Args:
            input_path: Input directory containing documents
            output_path: Output directory for merged files
            progress_callback: Optional callback function(current, total, message)
            
        Returns:
            Dict with merge statistics
        """
        print(f"\nAnalyzing folder structure: {input_path}")

        os.makedirs(output_path, exist_ok=True)

        input_abs = os.path.normcase(os.path.abspath(input_path))
        output_abs = os.path.normcase(os.path.abspath(output_path))
        exclude_paths = []
        if output_abs == input_abs or output_abs.startswith(input_abs + os.sep):
            exclude_paths.append(output_path)
            print(f"Excluding output folder from scan: {output_path}")

        # Analyze folder structure
        groups = self.folder_analyzer.analyze_structure(input_path, exclude_paths=exclude_paths)
        
        print(f"Found {len(groups)} groups to process")
        
        output_files = []
        file_count = 0
        warnings = []
        errors = []
        output_to_sources = {}
        word_conversion_summary = {
            'attempted': 0,
            'converted': 0,
            'failed': 0,
        }
        total_input_files = sum(len(files) for files in groups.values())
        
        for group_name, files in sorted(groups.items()):
            print(f"\nProcessing group: {group_name}")
            
            # Categorize files by type
            pdfs = [f for f in files if f.lower().endswith('.pdf')]
            word_docs = [f for f in files if f.lower().endswith(('.docx', '.doc'))]
            emails = [f for f in files if f.lower().endswith(('.msg', '.eml'))]
            
            # Merge PDFs
            if pdfs and self.process_pdfs:
                print(f"  Merging {len(pdfs)} PDF files...")
                required_pdf_outputs = self.pdf_merger.estimate_batch_count(pdfs)
                self._ensure_output_capacity(
                    required_pdf_outputs,
                    len(output_files),
                    f"group '{group_name}' PDF files",
                )
                pdf_outputs = self.pdf_merger.merge_pdfs(
                    pdfs,
                    output_path,
                    group_name,
                    warnings=warnings,
                )
                output_files.extend(pdf_outputs)
            
            # Process Word documents as PDF
            if word_docs and self.process_docx:
                print(f"  Processing {len(word_docs)} Word document files...")
                doc_outputs, doc_output_to_sources, conversion_summary = self._process_word_documents(
                    word_docs,
                    output_path,
                    group_name,
                    len(output_files),
                    warnings=warnings,
                )
                output_files.extend(doc_outputs)
                output_to_sources.update(doc_output_to_sources)
                word_conversion_summary['attempted'] += conversion_summary['attempted']
                word_conversion_summary['converted'] += conversion_summary['converted']
                word_conversion_summary['failed'] += conversion_summary['failed']
            
            # Merge emails
            if emails and self.process_emails:
                print(f"  Processing {len(emails)} email files...")
                email_threads = self._prepare_email_threads(emails, warnings=warnings)
                self._ensure_output_capacity(
                    len(email_threads),
                    len(output_files),
                    f"group '{group_name}' email threads",
                )
                email_outputs = self._write_email_threads(email_threads, output_path, group_name)
                output_files.extend(email_outputs)
            
            file_count += len(files)
            
            if progress_callback:
                progress_callback(file_count, total_input_files, f"Processed {group_name}")
        
        # Generate manifest
        manifest_path = os.path.join(output_path, 'merge_manifest.json')
        manifest = {
            'timestamp': datetime.now().isoformat(),
            'input_path': input_path,
            'total_input_files': total_input_files,
            'total_output_files': len(output_files),
            'output_files': output_files,
            'limits': {
                'max_file_size_kb': self.max_file_size_kb,
                'max_output_files': self.max_output_files,
            },
        }

        if warnings:
            manifest['warnings'] = warnings
        if errors:
            manifest['errors'] = errors
        if output_to_sources:
            manifest['output_to_sources'] = output_to_sources
        if word_conversion_summary['attempted'] > 0:
            manifest['word_conversion'] = word_conversion_summary
        
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)
        
        return manifest

    def _process_word_documents(
        self,
        word_files: List[str],
        output_path: str,
        group_name: str,
        current_output_count: int,
        warnings: Optional[List[Dict]] = None,
    ) -> Tuple[List[str], Dict[str, List[str]], Dict[str, int]]:
        """Convert .doc/.docx files to PDF, then merge converted PDFs."""
        word_files = sorted(word_files)
        conversion_summary = {
            'attempted': len(word_files),
            'converted': 0,
            'failed': 0,
        }

        available, reason = self.word_converter_factory.is_available()
        if not available:
            raise RuntimeError(
                "Word document processing requires Microsoft Word automation. "
                f"Details: {reason}"
            )

        conversion_dir = _make_writable_temp_dir(prefix=f"word_pdf_{group_name}_")
        converted_pdf_files: List[str] = []
        source_file_map: Dict[str, str] = {}
        bookmark_titles: Dict[str, str] = {}

        try:
            with self.word_converter_factory(warnings=warnings) as converter:
                for source_file in word_files:
                    converted_pdf = os.path.join(conversion_dir, f"{uuid.uuid4().hex}.pdf")
                    converted = converter.convert_file(source_file, converted_pdf)
                    if converted:
                        conversion_summary['converted'] += 1
                        converted_pdf_files.append(converted_pdf)
                        source_file_map[converted_pdf] = source_file
                        bookmark_titles[converted_pdf] = os.path.basename(source_file)
                    else:
                        conversion_summary['failed'] += 1

            if not converted_pdf_files:
                _record_warning(
                    warnings,
                    'word_conversion_no_outputs',
                    'No Word documents could be converted to PDF in this group',
                    group=group_name,
                    attempted=len(word_files),
                )
                return [], {}, conversion_summary

            required_outputs = self.pdf_merger.estimate_batch_count(converted_pdf_files)
            self._ensure_output_capacity(
                required_outputs,
                current_output_count,
                f"group '{group_name}' Word document files",
            )

            output_to_sources: Dict[str, List[str]] = {}
            doc_outputs = self.pdf_merger.merge_pdfs(
                converted_pdf_files,
                output_path,
                group_name,
                warnings=warnings,
                output_label="documents",
                bookmark_titles=bookmark_titles,
                source_file_map=source_file_map,
                output_to_sources=output_to_sources,
            )
            return doc_outputs, output_to_sources, conversion_summary
        finally:
            shutil.rmtree(conversion_dir, ignore_errors=True)

    def _ensure_output_capacity(
        self,
        required_outputs: int,
        current_output_count: int,
        context: str,
    ) -> None:
        """Validate that writing the next outputs will stay under the global limit."""
        if required_outputs <= 0:
            return

        remaining = self.max_output_files - current_output_count
        if required_outputs > remaining:
            raise RuntimeError(
                f"Output file limit exceeded before processing {context}: "
                f"requires {required_outputs} file(s), but only {remaining} slot(s) remain "
                f"(max_output_files={self.max_output_files})."
            )

    def _prepare_email_threads(
        self,
        email_files: List[str],
        warnings: Optional[List[Dict]] = None,
    ) -> Dict[str, List[Dict]]:
        """Extract and group email files into conversation threads."""
        email_data = []
        
        for email_file in email_files:
            if email_file.lower().endswith('.msg'):
                data = self.email_extractor.extract_msg(email_file)
            else:
                data = self.email_extractor.extract_eml(email_file)
            
            if data:
                data['file_path'] = email_file
                email_data.append(data)
            else:
                _record_warning(
                    warnings,
                    'email_extract_failed',
                    'Could not parse email file; skipping file',
                    file=email_file,
                )
        
        # Group into threads
        return self.email_threader.group_emails(email_data)

    def _write_email_threads(
        self,
        threads: Dict[str, List[Dict]],
        output_path: str,
        group_name: str,
    ) -> List[str]:
        """Write grouped email threads to text files."""
        os.makedirs(output_path, exist_ok=True)

        # Write thread files
        output_files = []
        for thread_num, (_, emails) in enumerate(sorted(threads.items()), 1):
            output_file = os.path.join(output_path, f"{group_name}_emails_thread{thread_num}.txt")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"EMAIL THREAD {thread_num}\n")
                f.write(f"GROUP: {group_name}\n")
                f.write(f"TOTAL EMAILS: {len(emails)}\n")
                f.write("=" * 80 + "\n\n")
                
                for idx, email in enumerate(emails, 1):
                    f.write(f"EMAIL {idx} of {len(emails)}\n")
                    f.write("=" * 80 + "\n")
                    f.write(f"Subject: {email.get('subject', '')}\n")
                    f.write(f"From: {email.get('from', '')}\n")
                    f.write(f"To: {email.get('to', '')}\n")
                    f.write(f"Date: {email.get('date', '')}\n")
                    f.write(f"Source: {os.path.basename(email.get('file_path', ''))}\n")
                    f.write("-" * 80 + "\n\n")
                    f.write(email.get('body', '') + "\n\n")
                    f.write("=" * 80 + "\n\n")
            
            output_files.append(output_file)
            print(f"    Created: {os.path.basename(output_file)} ({len(emails)} emails)")
        
        return output_files

    def _process_emails(self, email_files: List[str], output_path: str, group_name: str) -> List[str]:
        """Backward-compatible wrapper for email processing."""
        email_threads = self._prepare_email_threads(email_files)
        return self._write_email_threads(email_threads, output_path, group_name)
