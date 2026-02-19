"""
NotebookLM Document Merger - GUI Application
User-friendly interface for merging documents into NotebookLM-compatible batches
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import os
import platform
import subprocess
from pathlib import Path
from merger_engine import MergeOrchestrator


class DocumentMergerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("NotebookLM Document Merger v2.0")
        self.root.geometry("600x550")
        self.root.resizable(False, False)
        
        # Variables
        self.input_folder = tk.StringVar()
        self.output_folder = tk.StringVar()
        # Default batch target: 100 MB (in KB)
        self.max_file_size = tk.IntVar(value=102400)
        self.max_output_files = tk.IntVar(value=300)
        
        self.process_pdfs = tk.BooleanVar(value=True)
        self.process_docx = tk.BooleanVar(value=True)
        self.process_emails = tk.BooleanVar(value=True)
        
        self.is_processing = False
        
        # Build UI
        self.create_widgets()
        
    def create_widgets(self):
        """Create all UI widgets"""
        
        # Header
        header_frame = tk.Frame(self.root, bg='#2E86AB', height=60)
        header_frame.pack(fill=tk.X)
        header_frame.pack_propagate(False)
        
        title_label = tk.Label(
            header_frame,
            text="📄 NotebookLM Document Merger",
            font=('Arial', 18, 'bold'),
            bg='#2E86AB',
            fg='white'
        )
        title_label.pack(pady=15)
        
        # Main content frame
        content_frame = tk.Frame(self.root, padx=20, pady=20)
        content_frame.pack(fill=tk.BOTH, expand=True)
        
        # Input folder selection
        tk.Label(content_frame, text="Input Folder:", font=('Arial', 10, 'bold')).grid(
            row=0, column=0, sticky='w', pady=(0, 5)
        )
        
        input_frame = tk.Frame(content_frame)
        input_frame.grid(row=1, column=0, columnspan=2, sticky='ew', pady=(0, 15))
        
        tk.Entry(input_frame, textvariable=self.input_folder, width=50, state='readonly').pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10)
        )
        tk.Button(input_frame, text="Browse...", command=self.browse_input, width=10).pack(side=tk.RIGHT)
        
        # Output folder selection
        tk.Label(content_frame, text="Output Folder:", font=('Arial', 10, 'bold')).grid(
            row=2, column=0, sticky='w', pady=(0, 5)
        )
        
        output_frame = tk.Frame(content_frame)
        output_frame.grid(row=3, column=0, columnspan=2, sticky='ew', pady=(0, 15))
        
        tk.Entry(output_frame, textvariable=self.output_folder, width=50, state='readonly').pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10)
        )
        tk.Button(output_frame, text="Browse...", command=self.browse_output, width=10).pack(side=tk.RIGHT)
        
        # File type options
        options_frame = tk.LabelFrame(content_frame, text="File Types to Process", padx=10, pady=10)
        options_frame.grid(row=4, column=0, columnspan=2, sticky='ew', pady=(0, 15))
        
        tk.Checkbutton(options_frame, text="📄 PDF Files", variable=self.process_pdfs).grid(
            row=0, column=0, sticky='w', padx=10
        )
        tk.Checkbutton(options_frame, text="📝 Word Documents (.docx, .doc)", variable=self.process_docx).grid(
            row=0, column=1, sticky='w', padx=10
        )
        tk.Checkbutton(options_frame, text="📧 Email Files (.msg, .eml)", variable=self.process_emails).grid(
            row=0, column=2, sticky='w', padx=10
        )
        
        # Settings
        settings_frame = tk.LabelFrame(content_frame, text="Settings", padx=10, pady=10)
        settings_frame.grid(row=5, column=0, columnspan=2, sticky='ew', pady=(0, 15))
        
        tk.Label(settings_frame, text="Max File Size (KB):").grid(row=0, column=0, sticky='w', padx=(0, 10))
        tk.Spinbox(settings_frame, from_=1, to=99999999, textvariable=self.max_file_size, width=10).grid(
            row=0, column=1, sticky='w'
        )
        
        tk.Label(settings_frame, text="Max Output Files:").grid(row=0, column=2, sticky='w', padx=(20, 10))
        tk.Spinbox(settings_frame, from_=10, to=1000, textvariable=self.max_output_files, width=10).grid(
            row=0, column=3, sticky='w'
        )
        
        # Start button
        self.start_button = tk.Button(
            content_frame,
            text="🚀 Start Merging",
            command=self.start_merge,
            bg='#2E86AB',
            fg='white',
            font=('Arial', 12, 'bold'),
            height=2,
            cursor='hand2'
        )
        self.start_button.grid(row=6, column=0, columnspan=2, sticky='ew', pady=(0, 15))
        
        # Status label
        self.status_label = tk.Label(content_frame, text="Status: Ready", fg='#666')
        self.status_label.grid(row=7, column=0, columnspan=2, sticky='w', pady=(0, 5))
        
        # Progress bar
        self.progress = ttk.Progressbar(content_frame, mode='indeterminate')
        self.progress.grid(row=8, column=0, columnspan=2, sticky='ew', pady=(0, 10))
        
        # Statistics frame
        stats_frame = tk.Frame(content_frame)
        stats_frame.grid(row=9, column=0, columnspan=2, sticky='ew')
        
        self.files_found_label = tk.Label(stats_frame, text="Files Found: 0", fg='#666')
        self.files_found_label.grid(row=0, column=0, sticky='w')
        
        self.files_processed_label = tk.Label(stats_frame, text="Files Processed: 0", fg='#666')
        self.files_processed_label.grid(row=0, column=1, sticky='w', padx=(20, 0))
        
        self.output_files_label = tk.Label(stats_frame, text="Output Files: 0", fg='#666')
        self.output_files_label.grid(row=0, column=2, sticky='w', padx=(20, 0))
        
    def browse_input(self):
        """Open folder browser for input directory"""
        folder = filedialog.askdirectory(title="Select Input Folder")
        if folder:
            self.input_folder.set(folder)
            # Auto-suggest output folder
            if not self.output_folder.get():
                suggested_output = os.path.join(folder, "merged_output")
                self.output_folder.set(suggested_output)
    
    def browse_output(self):
        """Open folder browser for output directory"""
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.output_folder.set(folder)
    
    def start_merge(self):
        """Start the merging process"""
        # Validation
        if not self.input_folder.get():
            messagebox.showerror("Error", "Please select an input folder")
            return
        
        if not self.output_folder.get():
            messagebox.showerror("Error", "Please select an output folder")
            return
        
        if not any([self.process_pdfs.get(), self.process_docx.get(), self.process_emails.get()]):
            messagebox.showerror("Error", "Please select at least one file type to process")
            return
        
        if not os.path.exists(self.input_folder.get()):
            messagebox.showerror("Error", "Input folder does not exist")
            return
        
        # Start processing in a separate thread
        self.is_processing = True
        self.start_button.config(state='disabled', text='Processing...')
        self.progress.start(10)
        self.status_label.config(text="Status: Processing...", fg='#2E86AB')
        
        thread = threading.Thread(target=self.run_merge, daemon=True)
        thread.start()
    
    def run_merge(self):
        """Run the merge operation (in separate thread)"""
        try:
            # Create merger
            orchestrator = MergeOrchestrator(
                max_file_size_kb=self.max_file_size.get(),
                max_output_files=self.max_output_files.get(),
                process_pdfs=self.process_pdfs.get(),
                process_docx=self.process_docx.get(),
                process_emails=self.process_emails.get()
            )
            
            # Run merge
            result = orchestrator.merge_documents(
                self.input_folder.get(),
                self.output_folder.get()
            )
            
            # Update UI on success
            self.root.after(0, self.on_merge_complete, result)
            
        except Exception as e:
            # Update UI on error
            self.root.after(0, self.on_merge_error, str(e))
    
    def on_merge_complete(self, result):
        """Called when merge completes successfully"""
        self.progress.stop()
        self.is_processing = False
        self.start_button.config(state='normal', text='🚀 Start Merging')
        self.status_label.config(text="Status: Complete! ✓", fg='#28a745')
        
        # Update statistics
        self.files_found_label.config(text=f"Files Found: {result['total_input_files']}")
        self.files_processed_label.config(text=f"Files Processed: {result['total_input_files']}")
        self.output_files_label.config(text=f"Output Files: {result['total_output_files']}")
        
        # Show success message
        messagebox.showinfo(
            "Success",
            f"Merge complete!\n\n"
            f"Input files: {result['total_input_files']}\n"
            f"Output files: {result['total_output_files']}\n\n"
            f"Output saved to:\n{self.output_folder.get()}"
        )
        
        # Ask if user wants to open output folder
        if messagebox.askyesno("Open Folder", "Would you like to open the output folder?"):
            folder_path = self.output_folder.get()
            try:
                if platform.system() == 'Windows':
                    os.startfile(folder_path)
                elif platform.system() == 'Darwin':  # macOS
                    subprocess.run(['open', folder_path], check=True)
                else:  # Linux and other Unix-like systems
                    subprocess.run(['xdg-open', folder_path], check=True)
            except (OSError, FileNotFoundError, subprocess.CalledProcessError):
                messagebox.showwarning("Cannot Open Folder",
                                      f"Output saved to:\n{folder_path}\n\n"
                                      f"Please open manually.")
    
    def on_merge_error(self, error_msg):
        """Called when merge encounters an error"""
        self.progress.stop()
        self.is_processing = False
        self.start_button.config(state='normal', text='🚀 Start Merging')
        self.status_label.config(text="Status: Error", fg='#dc3545')
        
        messagebox.showerror("Error", f"An error occurred during merging:\n\n{error_msg}")


def main():
    """Main entry point"""
    root = tk.Tk()
    DocumentMergerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
