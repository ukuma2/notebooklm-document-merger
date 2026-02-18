"""
Document Merger Engine - Core merging logic
Handles PDF, DOCX, and Email merging with flexible folder structure support
"""

import os
import json
from pathlib import Path
from datetime import datetime
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

from email import policy
from email.parser import BytesParser
from dateutil import parser as date_parser


class PDFMerger:
    """Merges multiple PDF files into batched output files"""
    
    def __init__(self, max_file_size_kb=800):
        self.max_file_size_kb = max_file_size_kb
        self.max_file_size_bytes = max_file_size_kb * 1024
        
    def merge_pdfs(self, pdf_files: List[str], output_path: str, group_name: str) -> List[str]:
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
            file_size = os.path.getsize(pdf_file)
            
            # If adding this file would exceed limit, save current batch
            if current_batch and (current_batch_size + file_size > self.max_file_size_bytes):
                output_file = self._save_pdf_batch(current_batch, output_path, group_name, batch_num)
                output_files.append(output_file)
                batch_num += 1
                current_batch = []
                current_batch_size = 0
            
            current_batch.append(pdf_file)
            current_batch_size += file_size
        
        # Save remaining files
        if current_batch:
            output_file = self._save_pdf_batch(current_batch, output_path, group_name, batch_num)
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


    def _save_pdf_batch(self, pdf_files: List[str], output_path: str, group_name: str, batch_num: int) -> str:
        """Save a batch of PDFs into a single merged PDF"""
        writer = PdfWriter()
        
        # Add all pages from all PDFs
        for pdf_file in pdf_files:
            try:
                reader = PdfReader(pdf_file)
                for page in reader.pages:
                    writer.add_page(page)
            except Exception as e:
                # Fallback 1: File may be an image with a .pdf extension
                pdf_bytes = self._try_convert_image_to_pdf(pdf_file)
                if not pdf_bytes:
                    # Fallback 2: File may be an OLE .doc with a .pdf extension
                    pdf_bytes = self._try_convert_ole_doc_to_pdf(pdf_file)
                if pdf_bytes:
                    try:
                        reader = PdfReader(io.BytesIO(pdf_bytes))
                        for page in reader.pages:
                            writer.add_page(page)
                    except Exception as e2:
                        print(f"Warning: Could not merge {pdf_file} even after conversion: {e2}")
                else:
                    print(f"Warning: Could not merge {pdf_file}: {e}")
        
        # Generate output filename
        output_filename = f"{group_name}_pdfs_batch{batch_num}.pdf"
        output_file = os.path.join(output_path, output_filename)
        
        # Write merged PDF
        with open(output_file, 'wb') as f:
            writer.write(f)
        
        print(f"    Created: {output_filename} ({len(pdf_files)} PDFs)")
        return output_file


class DOCXMerger:
    """Merges multiple DOCX files into batched output files"""
    
    def __init__(self, max_file_size_kb=800):
        self.max_file_size_kb = max_file_size_kb
        self.max_file_size_bytes = max_file_size_kb * 1024
        
    def merge_docx(self, docx_files: List[str], output_path: str, group_name: str) -> List[str]:
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
            file_size = os.path.getsize(docx_file)
            
            # Check if we need to start a new batch
            if current_batch and (current_batch_size + file_size > self.max_file_size_bytes):
                output_file = self._save_docx_batch(current_batch, output_path, group_name, batch_num)
                output_files.append(output_file)
                batch_num += 1
                current_batch = []
                current_batch_size = 0
            
            current_batch.append(docx_file)
            current_batch_size += file_size
        
        # Save remaining files
        if current_batch:
            output_file = self._save_docx_batch(current_batch, output_path, group_name, batch_num)
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
            ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
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

    def _save_docx_batch(self, docx_files: List[str], output_path: str, group_name: str, batch_num: int) -> str:
        """Save a batch of DOCX files into a single merged document"""
        import tempfile
        import shutil

        merged_doc = Document()

        for idx, docx_file in enumerate(docx_files):
            # Add document separator header
            if idx > 0:
                merged_doc.add_page_break()
            heading = merged_doc.add_heading(level=1)
            heading.text = f"Document: {os.path.basename(docx_file)}"

            source_doc = None

            # --- Attempt 1: open normally ---
            try:
                source_doc = Document(docx_file)
            except Exception as e1:
                # --- Attempt 2: copy to a temp .docx and retry ---
                # Handles .doc files that are actually OOXML (wrong extension)
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
                    # --- Attempt 3: extract raw text from the zip's document.xml ---
                    raw_text = self._try_extract_docx_text(docx_file)
                    if not raw_text:
                        # --- Attempt 4: extract text from OLE compound document ---
                        raw_text = self._try_extract_ole_text(docx_file)
                        if raw_text:
                            print(f"    Recovered (OLE text): {os.path.basename(docx_file)}")
                        else:
                            print(f"Warning: Could not merge {docx_file}: {e1}")
                            merged_doc.add_paragraph(f"[Error reading file: {os.path.basename(docx_file)}]")
                            continue
                    else:
                        print(f"    Recovered (raw text): {os.path.basename(docx_file)}")
                    merged_doc.add_paragraph(raw_text)
                    continue

            if source_doc is not None:
                try:
                    for element in source_doc.element.body:
                        merged_doc.element.body.append(element)
                except Exception as e:
                    print(f"Warning: Could not append content from {docx_file}: {e}")
                    merged_doc.add_paragraph(f"[Error appending content: {os.path.basename(docx_file)}]")
        
        # Generate output filename
        output_filename = f"{group_name}_documents_batch{batch_num}.docx"
        output_file = os.path.join(output_path, output_filename)
        
        # Save merged document
        merged_doc.save(output_file)
        
        print(f"    Created: {output_filename} ({len(docx_files)} documents)")
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
            thread_emails.sort(key=lambda e: e.get('date') or datetime.min)
        
        return dict(threads)


class FolderAnalyzer:
    """Analyzes folder structure and determines grouping strategy"""
    
    @staticmethod
    def analyze_structure(root_path: str) -> Dict[str, List[str]]:
        """
        Analyze folder structure and group files by parent folder
        
        Args:
            root_path: Root directory to analyze
            
        Returns:
            Dict mapping group_name to list of file paths
        """
        groups = defaultdict(list)
        
        for dirpath, dirnames, filenames in os.walk(root_path):
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
    
    def __init__(self, max_file_size_kb=800, max_output_files=300, 
                 process_pdfs=True, process_docx=True, process_emails=True):
        self.pdf_merger = PDFMerger(max_file_size_kb)
        self.docx_merger = DOCXMerger(max_file_size_kb)
        self.email_extractor = EmailExtractor()
        self.email_threader = EmailThreader()
        self.folder_analyzer = FolderAnalyzer()
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
        
        # Analyze folder structure
        groups = self.folder_analyzer.analyze_structure(input_path)
        
        print(f"Found {len(groups)} groups to process")
        
        output_files = []
        file_count = 0
        
        for group_name, files in sorted(groups.items()):
            print(f"\nProcessing group: {group_name}")
            
            # Categorize files by type
            pdfs = [f for f in files if f.lower().endswith('.pdf')]
            docx = [f for f in files if f.lower().endswith(('.docx', '.doc'))]
            emails = [f for f in files if f.lower().endswith(('.msg', '.eml'))]
            
            # Merge PDFs
            if pdfs and self.process_pdfs:
                print(f"  Merging {len(pdfs)} PDF files...")
                pdf_outputs = self.pdf_merger.merge_pdfs(pdfs, output_path, group_name)
                output_files.extend(pdf_outputs)
            
            # Merge DOCX
            if docx and self.process_docx:
                print(f"  Merging {len(docx)} document files...")
                docx_outputs = self.docx_merger.merge_docx(docx, output_path, group_name)
                output_files.extend(docx_outputs)
            
            # Merge emails
            if emails and self.process_emails:
                print(f"  Processing {len(emails)} email files...")
                email_outputs = self._process_emails(emails, output_path, group_name)
                output_files.extend(email_outputs)
            
            file_count += len(files)
            
            if progress_callback:
                progress_callback(file_count, len(files), f"Processed {group_name}")
        
        # Generate manifest
        manifest_path = os.path.join(output_path, 'merge_manifest.json')
        manifest = {
            'timestamp': datetime.now().isoformat(),
            'input_path': input_path,
            'total_input_files': file_count,
            'total_output_files': len(output_files),
            'output_files': output_files
        }
        
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        return manifest
    
    def _process_emails(self, email_files: List[str], output_path: str, group_name: str) -> List[str]:
        """Process and thread email files"""
        email_data = []
        
        for email_file in email_files:
            if email_file.lower().endswith('.msg'):
                data = self.email_extractor.extract_msg(email_file)
            else:
                data = self.email_extractor.extract_eml(email_file)
            
            if data:
                data['file_path'] = email_file
                email_data.append(data)
        
        # Group into threads
        threads = self.email_threader.group_emails(email_data)
        
        # Write thread files
        output_files = []
        for thread_num, (thread_key, emails) in enumerate(sorted(threads.items()), 1):
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
