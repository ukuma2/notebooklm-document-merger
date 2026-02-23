"""
NotebookLM Document Merger - GUI Application
User-friendly interface for merging documents into NotebookLM-compatible batches
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
from collections import deque
import os
import platform
import subprocess
from pathlib import Path
from merger_engine import MergeOrchestrator, WordToPdfConverter

_LOG_MAX_LINES = 5000
_LOG_TRIM_LINES = 1000


class DocumentMergerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("NotebookLM Document Merger v2.0")
        self.root.geometry("900x760")
        self.root.resizable(True, True)

        # Variables
        self.input_folder = tk.StringVar()
        self.output_folder = tk.StringVar()
        # Default batch target: 100 MB (in KB)
        self.max_file_size = tk.IntVar(value=102400)
        self.max_output_files = tk.IntVar(value=300)

        self.process_pdfs = tk.BooleanVar(value=True)
        self.process_docx = tk.BooleanVar(value=True)
        self.process_emails = tk.BooleanVar(value=True)
        self.unprocessed_count_var = tk.IntVar(value=0)
        self.failed_count_var = tk.IntVar(value=0)
        self.skipped_count_var = tk.IntVar(value=0)
        self.recent_paths_var = tk.StringVar(value="Recent moved/failed files: none")
        self.recent_paths = deque(maxlen=10)

        self.is_processing = False
        self.cancel_event = threading.Event()
        self._merge_thread = None

        # Build UI
        self.create_widgets()
        self._check_word_availability()

        # Safe window close handler (H1)
        self.root.protocol("WM_DELETE_WINDOW", self._on_window_close)

    def create_widgets(self):
        """Create all UI widgets"""

        # Header
        header_frame = tk.Frame(self.root, bg='#2E86AB', height=60)
        header_frame.pack(fill=tk.X)
        header_frame.pack_propagate(False)

        title_label = tk.Label(
            header_frame,
            text="NotebookLM Document Merger",
            font=('Arial', 18, 'bold'),
            bg='#2E86AB',
            fg='white'
        )
        title_label.pack(pady=15)

        # Main content frame
        content_frame = tk.Frame(self.root, padx=20, pady=20)
        content_frame.pack(fill=tk.BOTH, expand=True)
        content_frame.grid_columnconfigure(0, weight=1)
        content_frame.grid_columnconfigure(1, weight=1)
        content_frame.grid_rowconfigure(12, weight=1)

        # Input folder/zip selection
        tk.Label(content_frame, text="Input Folder or ZIP:", font=('Arial', 10, 'bold')).grid(
            row=0, column=0, sticky='w', pady=(0, 5)
        )

        input_frame = tk.Frame(content_frame)
        input_frame.grid(row=1, column=0, columnspan=2, sticky='ew', pady=(0, 15))

        tk.Entry(input_frame, textvariable=self.input_folder, width=50, state='readonly').pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10)
        )
        input_buttons = tk.Frame(input_frame)
        input_buttons.pack(side=tk.RIGHT)
        tk.Button(input_buttons, text="ZIP...", command=self.browse_input_zip, width=8).pack(side=tk.RIGHT)
        tk.Button(input_buttons, text="Folder...", command=self.browse_input, width=8).pack(side=tk.RIGHT, padx=(0, 6))

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

        tk.Checkbutton(options_frame, text="PDF Files", variable=self.process_pdfs).grid(
            row=0, column=0, sticky='w', padx=10
        )
        self.docx_checkbox = tk.Checkbutton(
            options_frame, text="Word Documents (.docx, .doc)", variable=self.process_docx
        )
        self.docx_checkbox.grid(row=0, column=1, sticky='w', padx=10)
        tk.Checkbutton(options_frame, text="Email Files (.msg, .eml)", variable=self.process_emails).grid(
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

        # Button frame (Start + Cancel side-by-side)
        button_frame = tk.Frame(content_frame)
        button_frame.grid(row=6, column=0, columnspan=2, sticky='ew', pady=(0, 15))
        button_frame.grid_columnconfigure(0, weight=3)
        button_frame.grid_columnconfigure(1, weight=1)

        self.start_button = tk.Button(
            button_frame,
            text="Start Merging",
            command=self.start_merge,
            bg='#2E86AB',
            fg='white',
            font=('Arial', 12, 'bold'),
            height=2,
            cursor='hand2'
        )
        self.start_button.grid(row=0, column=0, sticky='ew', padx=(0, 8))

        self.cancel_button = tk.Button(
            button_frame,
            text="Cancel",
            command=self._request_cancel,
            bg='#dc3545',
            fg='white',
            font=('Arial', 12, 'bold'),
            height=2,
            state='disabled',
        )
        self.cancel_button.grid(row=0, column=1, sticky='ew')

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

        self.unprocessed_label = tk.Label(stats_frame, text="Unprocessed Relocated: 0", fg='#666')
        self.unprocessed_label.grid(row=1, column=0, sticky='w', pady=(6, 0))

        self.failed_label = tk.Label(stats_frame, text="Failed Files: 0", fg='#666')
        self.failed_label.grid(row=1, column=1, sticky='w', padx=(20, 0), pady=(6, 0))

        self.skipped_label = tk.Label(stats_frame, text="Skipped Files: 0", fg='#666')
        self.skipped_label.grid(row=1, column=2, sticky='w', padx=(20, 0), pady=(6, 0))

        self.recent_paths_label = tk.Label(
            content_frame,
            textvariable=self.recent_paths_var,
            fg='#666',
            justify='left',
            anchor='w',
            wraplength=820,
        )
        self.recent_paths_label.grid(row=10, column=0, columnspan=2, sticky='ew', pady=(10, 5))

        tk.Label(content_frame, text="Live Run Log:", font=('Arial', 10, 'bold')).grid(
            row=11, column=0, columnspan=2, sticky='w', pady=(4, 4)
        )

        log_frame = tk.Frame(content_frame)
        log_frame.grid(row=12, column=0, columnspan=2, sticky='nsew')
        log_frame.grid_columnconfigure(0, weight=1)
        log_frame.grid_rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, height=14, wrap='word', state='disabled')
        self.log_text.grid(row=0, column=0, sticky='nsew')
        log_scroll = ttk.Scrollbar(log_frame, orient='vertical', command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky='ns')
        self.log_text.configure(yscrollcommand=log_scroll.set)

    def _check_word_availability(self):
        """Disable Word checkbox if Microsoft Word is not available on this machine."""
        available, reason = WordToPdfConverter.is_available()
        if not available:
            self.process_docx.set(False)
            self.docx_checkbox.config(
                state='disabled',
                text=f"Word Documents — not available ({reason})",
            )

    def _on_window_close(self):
        """Handle window close (X button). Confirm if merge is running."""
        if self.is_processing:
            if messagebox.askyesno(
                "Merge in progress",
                "A merge is currently running.\n\nCancel the merge and close?",
            ):
                self.cancel_event.set()
                self.status_label.config(text="Status: Cancelling...", fg='#dc3545')
                # Wait briefly for the thread to notice cancellation.
                if self._merge_thread is not None:
                    self._merge_thread.join(timeout=5)
                self.root.destroy()
        else:
            self.root.destroy()

    def _request_cancel(self):
        """User clicked Cancel button."""
        if not self.is_processing:
            return
        self.cancel_event.set()
        self.cancel_button.config(state='disabled', text='Cancelling...')
        self.status_label.config(text="Status: Cancelling...", fg='#dc3545')
        self._append_log("[INFO] Cancel requested. Waiting for current operation to finish...")

    def browse_input(self):
        """Open folder browser for input directory"""
        folder = filedialog.askdirectory(title="Select Input Folder")
        if folder:
            self._set_input_path(folder)

    def browse_input_zip(self):
        """Open file browser for ZIP input."""
        zip_file = filedialog.askopenfilename(
            title="Select ZIP File",
            filetypes=[("ZIP files", "*.zip"), ("All files", "*.*")],
        )
        if zip_file:
            self._set_input_path(zip_file)

    def _set_input_path(self, path):
        self.input_folder.set(path)
        # Always refresh output suggestion when input changes.
        if os.path.isfile(path) and path.lower().endswith('.zip'):
            zip_name = Path(path).stem
            suggested_output = os.path.join(os.path.dirname(path), f"{zip_name}_merged_output")
        else:
            suggested_output = os.path.join(path, "merged_output")
        self.output_folder.set(suggested_output)

    def browse_output(self):
        """Open folder browser for output directory"""
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.output_folder.set(folder)

    def _append_log(self, line):
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, line + "\n")
        # H8: Log rotation — trim oldest lines when text gets too large.
        line_count = int(self.log_text.index('end-1c').split('.')[0])
        if line_count > _LOG_MAX_LINES:
            self.log_text.delete('1.0', f'{_LOG_TRIM_LINES + 1}.0')
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def _reset_live_state(self):
        self.unprocessed_count_var.set(0)
        self.failed_count_var.set(0)
        self.skipped_count_var.set(0)
        self.unprocessed_label.config(text="Unprocessed Relocated: 0")
        self.failed_label.config(text="Failed Files: 0")
        self.skipped_label.config(text="Skipped Files: 0")
        self.recent_paths.clear()
        self.recent_paths_var.set("Recent moved/failed files: none")
        self.log_text.config(state='normal')
        self.log_text.delete("1.0", tk.END)
        self.log_text.config(state='disabled')

    def _update_recent_paths(self):
        if not self.recent_paths:
            self.recent_paths_var.set("Recent moved/failed files: none")
            return
        rendered = "\n".join(f"- {item}" for item in list(self.recent_paths))
        self.recent_paths_var.set(f"Recent moved/failed files:\n{rendered}")

    def on_progress_update(self, current, total, message):
        try:
            self.root.after(0, self._handle_progress_update, current, total, message)
        except Exception:
            pass  # Window may have been destroyed

    def _handle_progress_update(self, current, total, message):
        self.status_label.config(text=f"Status: {message}", fg='#2E86AB')
        self.files_processed_label.config(text=f"Files Processed: {current}/{max(total, 1)}")
        self._append_log(f"[PROGRESS] {message} ({current}/{total})")

    def on_run_event(self, payload):
        try:
            self.root.after(0, self._handle_run_event, payload)
        except Exception:
            pass  # Window may have been destroyed

    def _handle_run_event(self, payload):
        if not isinstance(payload, dict):
            return
        level = str(payload.get("level", "INFO")).upper()
        event = str(payload.get("event", "event"))
        message = str(payload.get("message", ""))
        context = payload.get("context", {}) or {}
        self._append_log(f"[{level}] {event}: {message}")

        _ZIP_SKIP_EVENTS = {
            "zip_entry_skipped_unsafe_path",
            "zip_nested_depth_exceeded",
            "zip_empty_after_extraction",
        }
        _PROCESSING_FAILURE_EVENTS = {
            "word_to_pdf_failed",
            "word_to_pdf_timeout",
            "word_conversion_no_outputs",
            "email_extract_failed",
            "pdf_stat_failed",
            "pdf_unreadable",
            "pdf_encrypted",
            "pdf_conversion_failed",
            "failed_artifact_created",
        }

        if event in {"unsupported_input_file_relocated", "unsupported_zip_file_relocated"}:
            self.unprocessed_count_var.set(self.unprocessed_count_var.get() + 1)
        elif event in _ZIP_SKIP_EVENTS:
            self.skipped_count_var.set(self.skipped_count_var.get() + 1)
        elif event in _PROCESSING_FAILURE_EVENTS or (level in {"WARNING", "ERROR"} and event not in _ZIP_SKIP_EVENTS and "zip" not in event.lower()):
            self.failed_count_var.set(self.failed_count_var.get() + 1)

        self.unprocessed_label.config(text=f"Unprocessed Relocated: {self.unprocessed_count_var.get()}")
        self.failed_label.config(text=f"Failed Files: {self.failed_count_var.get()}")
        self.skipped_label.config(text=f"Skipped Files: {self.skipped_count_var.get()}")

        source = context.get("source") or context.get("file")
        destination = context.get("destination")
        if source or destination:
            if source and destination:
                self.recent_paths.append(f"{source} -> {destination}")
            else:
                self.recent_paths.append(str(source or destination))
            self._update_recent_paths()

    def start_merge(self):
        """Start the merging process"""
        # H10: Double-click safety guard
        if self.is_processing:
            return

        # H7: Input validation for spinbox values
        try:
            file_size = self.max_file_size.get()
            if file_size < 1:
                raise ValueError("must be >= 1")
        except (tk.TclError, ValueError):
            messagebox.showerror("Error", "Max File Size must be a positive number (in KB).")
            return

        try:
            output_files = self.max_output_files.get()
            if output_files < 1:
                raise ValueError("must be >= 1")
        except (tk.TclError, ValueError):
            messagebox.showerror("Error", "Max Output Files must be a positive number.")
            return

        if not self.input_folder.get():
            messagebox.showerror("Error", "Please select an input folder or ZIP file")
            return

        if not self.output_folder.get():
            messagebox.showerror("Error", "Please select an output folder")
            return

        if not any([self.process_pdfs.get(), self.process_docx.get(), self.process_emails.get()]):
            messagebox.showerror("Error", "Please select at least one file type to process")
            return

        input_path = self.input_folder.get()
        if not os.path.exists(input_path):
            messagebox.showerror("Error", "Input path does not exist")
            return

        is_folder = os.path.isdir(input_path)
        is_zip_file = os.path.isfile(input_path) and input_path.lower().endswith('.zip')
        if not (is_folder or is_zip_file):
            messagebox.showerror("Error", "Input must be a folder or a .zip file")
            return

        # Start processing in a separate thread
        self.is_processing = True
        self.cancel_event.clear()
        self.start_button.config(state='disabled', text='Processing...')
        self.cancel_button.config(state='normal', text='Cancel')
        self.progress.start(10)
        self.status_label.config(text="Status: Processing...", fg='#2E86AB')
        self._reset_live_state()
        self._append_log("Run started.")

        self._merge_thread = threading.Thread(target=self.run_merge, daemon=True)
        self._merge_thread.start()

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

            # Run merge with cancel_event support
            result = orchestrator.merge_documents(
                self.input_folder.get(),
                self.output_folder.get(),
                progress_callback=self.on_progress_update,
                event_callback=self.on_run_event,
                cancel_event=self.cancel_event,
            )

            # Update UI on success
            try:
                self.root.after(0, self.on_merge_complete, result)
            except Exception:
                pass

        except Exception as e:
            # Update UI on error
            try:
                self.root.after(0, self.on_merge_error, str(e))
            except Exception:
                pass

    def on_merge_complete(self, result):
        """Called when merge completes successfully"""
        self.progress.stop()
        self.is_processing = False
        self.start_button.config(state='normal', text='Start Merging')
        self.cancel_button.config(state='disabled', text='Cancel')

        was_cancelled = self.cancel_event.is_set()
        if was_cancelled:
            self.status_label.config(text="Status: Cancelled", fg='#dc3545')
        else:
            self.status_label.config(text="Status: Complete!", fg='#28a745')

        summary = result.get("summary", {})
        paths = result.get("paths", {})
        logs = result.get("logs", {})
        files = result.get("files", {})

        input_total = summary.get("input_files_total", result.get("total_input_files", 0))
        processed_total = summary.get("processed_outputs_total", result.get("total_output_files", 0))
        moved_total = summary.get("unprocessed_relocated_total", summary.get("moved_unprocessed_total", 0))
        failed_total = summary.get("failed_files_total", 0)
        failed_artifacts_total = summary.get("failed_artifacts_total", 0)
        skipped_total = summary.get("skipped_files_total", 0)

        # Update statistics
        self.files_found_label.config(text=f"Files Found: {input_total}")
        self.files_processed_label.config(text=f"Files Processed: {input_total - failed_total - skipped_total}")
        self.output_files_label.config(text=f"Output Files: {processed_total}")
        self.unprocessed_label.config(text=f"Unprocessed Relocated: {moved_total}")
        self.failed_label.config(text=f"Failed Files: {failed_total}")
        self.skipped_label.config(text=f"Skipped Files: {skipped_total}")
        for failed_item in files.get("failed", [])[:10]:
            source = failed_item.get("source")
            artifact = failed_item.get("artifact_destination")
            if source and artifact:
                self.recent_paths.append(f"{source} -> {artifact}")
            elif source:
                self.recent_paths.append(str(source))
        self._update_recent_paths()
        self._append_log("Run completed." if not was_cancelled else "Run cancelled.")

        failed_preview = ""
        if files.get("failed"):
            preview_lines = []
            for item in files["failed"][:3]:
                src = item.get("source", "unknown")
                status = item.get("artifact_status", "unknown")
                preview_lines.append(f"- {src} ({status})")
            failed_preview = "Failed file details:\n" + "\n".join(preview_lines) + "\n\n"

        # Show success message
        title = "Cancelled" if was_cancelled else "Success"
        messagebox.showinfo(
            title,
            f"{'Merge cancelled (partial results below).' if was_cancelled else 'Merge complete!'}\n\n"
            f"Input files: {input_total}\n"
            f"Processed outputs: {processed_total}\n"
            f"Unprocessed relocated: {moved_total}\n"
            f"Failed files: {failed_total}\n"
            f"Failed artifacts created: {failed_artifacts_total}\n"
            f"Skipped files: {skipped_total}\n\n"
            f"Processed folder:\n{paths.get('processed_dir', self.output_folder.get())}\n\n"
            f"Unprocessed folder:\n{paths.get('unprocessed_dir', self.output_folder.get())}\n\n"
            f"Failed folder:\n{paths.get('failed_dir', self.output_folder.get())}\n\n"
            f"Manifest:\n{os.path.join(paths.get('processed_dir', self.output_folder.get()), 'merge_manifest.json')}\n\n"
            f"Run log:\n{logs.get('text_log', 'N/A')}\n\n"
            f"{failed_preview}"
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
        self.start_button.config(state='normal', text='Start Merging')
        self.cancel_button.config(state='disabled', text='Cancel')
        self.status_label.config(text="Status: Error", fg='#dc3545')
        self._append_log(f"[ERROR] {error_msg}")

        # Truncate very long error messages for the dialog.
        display_msg = error_msg if len(error_msg) <= 1000 else error_msg[:1000] + "\n\n... (truncated, see run log for full error)"
        messagebox.showerror("Error", f"An error occurred during merging:\n\n{display_msg}")


def main():
    """Main entry point"""
    root = tk.Tk()
    DocumentMergerGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
