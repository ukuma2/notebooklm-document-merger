# NotebookLM Document Merger: Implementation Handover

## 1) Purpose and Business Context

### Why this was built
This tool was built to prepare large, mixed-format matter folders for NotebookLM ingestion with predictable outputs and auditability.

The key operational problems it addresses are:
- NotebookLM upload limits (file count and practical file-size constraints).
- Mixed source formats (PDF, Word, email exports) in the same intake.
- ZIP-only delivery from source systems.
- Windows extraction failures caused by long ZIP entry names.
- Need for clear run outcomes for internal support and non-technical users.

### Intended users
- Operations teams running bulk document preparation.
- Internal support teams troubleshooting run outcomes.
- Legal/admin users preparing folders for NotebookLM upload.

## 2) Current Scope of the Build

### Supported processing
- PDF files: native PDF merge with batching.
- Word files (`.doc`, `.docx`): converted through Microsoft Word automation to PDF, then merged.
- Email files (`.eml`, `.msg`): parsed, threaded by subject, written to size-batched text outputs.

### Supported inputs
- Folder input.
- Single `.zip` file input (GUI ZIP picker or orchestrator input path).

### Explicit non-goals (current release)
- OCR pipeline for scanned documents.
- Embedding binary attachment payloads into email text outputs.
- Nested ZIP traversal deeper than configured depth (default depth limit is 1).

## 3) Key Implemented Features

- Structured output contract on every run:
  - `processed/`
  - `unprocessed/`
  - `failed/`
  - `logs/`
- ZIP preprocessing integrated into merge flow:
  - long-name truncation (default 50 chars including extension),
  - zip-slip path blocking,
  - nested ZIP extraction with depth limit.
- Unsupported file relocation with manifest tracking.
- File-level failure tracking and failed artifact materialization.
- Email outputs switched to size-batched files (default 25 MB).
- Email outputs include attachment metadata index.
- GUI live run log with stage/events plus moved/failed recents.
- GUI auto-refreshes suggested output folder when input path changes (folder or ZIP).

## 4) Public Interface and Runtime Contract

## `MergeOrchestrator.__init__` key options
- Core controls:
  - `max_file_size_kb=102400`
  - `max_output_files=300`
  - `process_pdfs=True`
  - `process_docx=True`
  - `process_emails=True`
- ZIP controls:
  - `process_zip_archives=True`
  - `zip_max_filename_length=50`
  - `zip_include_extension_in_limit=True`
  - `zip_nested_depth_limit=1`
- Structured output controls:
  - `output_layout_mode="structured"`
  - `processed_subdir="processed"`
  - `unprocessed_subdir="unprocessed"`
  - `failed_subdir="failed"`
  - `logs_subdir="logs"`
- Logging controls:
  - `word_progress_interval=10`
  - `enable_detailed_logging=True`
  - `log_privacy_mode="redacted"`
- Relocation/artifact controls:
  - `relocate_unsupported_input_files=True`
  - `unsupported_input_action="copy"`
  - `failed_file_artifact_action="copy"`
  - `unprocessed_include_source_files=True`
  - `failed_include_artifacts=True`
- Email output controls:
  - `email_output_mode="size_batched"`
  - `email_max_output_file_mb=25`
  - `email_include_attachment_index=True`
  - `email_batch_name_prefix="emails_batch"`

## `merge_documents(...)` runtime behavior
- Signature includes:
  - `merge_documents(input_path, output_path, progress_callback=None, event_callback=None)`.
- `event_callback` receives structured run events from logger with:
  - level,
  - event code,
  - human message,
  - context payload (redacted by default).

### Manifest additions and contract
`processed/merge_manifest.json` includes:
- top-level:
  - `run_id`
  - `paths`
  - `summary`
  - `files`
  - `logs`
- optional sections based on run:
  - `emails`
  - `zip_processing`
  - `word_conversion`
  - `warnings`
  - `errors`

## 5) Output Folder Semantics

For each run under selected output root:

```text
<output_root>/
  processed/
    <group>_pdfs_batchN.pdf
    <group>_documents_batchN.pdf
    <group>_emails_batchN.txt
    merge_manifest.json
  unprocessed/
    <unsupported_or_nonprocessable_files_flattened_with_unique_suffixes>
  failed/
    <stage>/
      <failed_source_copy_or_move_if_available>
  logs/
    run_<run_id>.log
    run_<run_id>.jsonl
```

### Folder meaning
- `processed/`: all generated NotebookLM-ready outputs + manifest.
- `unprocessed/`: unsupported files relocated without transformation.
  - Current behavior is flattened destination naming with collision-safe suffixes.
- `failed/`: artifacts copied/moved from processable files that failed at file level.
- `logs/`: text + JSONL event logs for support/debug.

## 6) Processing Lifecycle and Error Model

### Lifecycle
1. Validate input path.
2. Bootstrap structured output folders and run logger.
3. Analyze folder structure (excluding output path if nested inside input).
4. Expand ZIP archives (subject to safety and depth controls).
5. Classify files:
   - processable (`.pdf`, `.doc`, `.docx`, `.eml`, `.msg`)
   - unsupported (relocate to `unprocessed/` if enabled).
6. Process by type:
   - PDF merge,
   - Word conversion + merge,
   - Email parse/thread/batch write.
7. Collect failures/skips from warning stream.
8. Attempt failed-artifact materialization.
9. Write manifest, close logs, clean temporary ZIP extraction dirs.

### Error policy
- File-level failures are non-fatal and run continues.
- Infrastructure-level failures are fatal (run raises with log and manifest location context).

### Common warning/error codes
- ZIP:
  - `zip_extract_failed`
  - `zip_entry_skipped_unsafe_path`
  - `zip_nested_depth_exceeded`
  - `zip_empty_after_extraction`
- File relocation/artifacts:
  - `unsupported_relocate_failed`
  - `failed_artifact_create_failed`
- Processing:
  - `word_to_pdf_failed`
  - `word_conversion_no_outputs`
  - `email_extract_failed`
  - `pdf_stat_failed`
  - `pdf_unreadable`
  - `pdf_conversion_failed`
  - `email_thread_exceeds_batch_cap`

Warnings surface in:
- GUI live log,
- `processed/merge_manifest.json`,
- `logs/run_<id>.log` and `.jsonl`.

### Failed artifact status values
- `created`: artifact successfully materialized in `failed/`.
- `source_missing`: source path unavailable/unreadable at artifact stage.
- `copy_failed`: attempted artifact action failed.
- `not_created`: artifact intentionally not created (configuration/metadata-only flow).

## 7) Email Output Behavior

- Emails are parsed and grouped by normalized subject thread.
- Threads are rendered into text blocks and packed into batch files up to `email_max_output_file_mb` (default 25 MB).
- Output naming uses `<group>_emails_batchN.txt` in size-batched mode.
- If one thread alone exceeds cap, it is placed in a dedicated batch and warning `email_thread_exceeds_batch_cap` is recorded.
- Each email block includes:
  - subject,
  - from/to/cc/date,
  - source filename,
  - body text,
  - attachment index (filename/content type/size where available).
- Binary attachment payloads are not embedded.

## 8) ZIP Behavior and Safety

- ZIP processing is enabled by default.
- ZIP leaf names are truncated to configured max length (default 50 chars, including extension).
- Name collisions are resolved with numeric suffixes.
- Unsafe ZIP entry paths are blocked (`..`, absolute paths, drive-prefixed paths).
- Nested ZIP extraction allowed only up to `zip_nested_depth_limit` (default 1).
- ZIP contents are processed under synthetic group names derived from the ZIP name.

## 9) Test Coverage Snapshot

Current suite includes targeted and regression tests for core behaviors:

- `tests/zip_processing_test.py`
  - long-name ZIP extraction,
  - truncation collision handling,
  - nested ZIP (supported and depth-exceeded),
  - zip-slip blocking,
  - mixed ZIP + plain input,
  - unsupported relocation from ZIP and normal input.
- `tests/email_output_batching_test.py`
  - size-capped email batching,
  - attachment index presence,
  - failed email artifact creation.
- `tests/orchestrator_reliability_test.py`
  - structured folder contract,
  - manifest/log existence,
  - output-folder scan exclusion,
  - progress callback totals.
- `tests/output_limit_integration_test.py`
  - max-output-files guardrail across content types.
- `tests/email_threading_test.py`
  - core thread grouping and ordering behavior.
- `tests/word_pdf_pipeline_test.py`
  - Word conversion flow,
  - skip/warning behavior on conversion failure,
  - bookmark/source mapping,
  - batching and progress cadence.
- `tests/gui_defaults_test.py`
  - GUI defaults and event callback wiring.

### Confidence statement
Coverage is strong for the implemented processing contract, ZIP safety behavior, structured outputs, and email batching logic.

### Residual testing gaps
- Large real-world archive variability still benefits from periodic smoke runs.
- Word automation edge cases depend on local Microsoft Word installation state/version.
- OS-level long-path and permission edge cases can still vary by endpoint policy.

## 10) Known Limits and Operational Guidance

- Word conversion requires Windows plus Microsoft Word (COM automation).
- Malformed/encrypted PDFs can be skipped with warnings.
- Very large datasets can take significant time; use live GUI log and `logs/run_<id>.log`.
- Filename/path constraints on Windows are mitigated, but not fully eliminated for all endpoint policies.
- Logging defaults to privacy-redacted mode; use full mode only for controlled support scenarios.

## 11) Firm-Wide Rollout Guidance (High-Level)

Recommended rollout model:
1. Package as signed Windows executable (PyInstaller) for non-technical users.
2. Maintain centralized versioning and release notes.
3. Publish a support runbook with required incident bundle:
   - `processed/merge_manifest.json`
   - `logs/run_<id>.log`
   - optional `logs/run_<id>.jsonl`
4. Use pilot group rollout before full deployment.

## 12) Quick Troubleshooting Playbook

### Missing files after run
- Check `processed/` for generated outputs.
- Check `unprocessed/` for unsupported relocated files.
- Check `failed/` for failed processable file artifacts.
- Check `processed/merge_manifest.json` summary and `files` sections.

### “Failed files > 0” but no artifact
- Inspect `files.failed[*].artifact_status` in manifest.
- `source_missing` usually indicates inaccessible or non-resolvable source path at artifact stage.
- Check `logs/run_<id>.log` for copy/move errors.

### ZIP input oddities
- Check `zip_processing` counters in manifest.
- Check warnings for `zip_entry_skipped_unsafe_path`, `zip_nested_depth_exceeded`, `zip_extract_failed`.

### Slow run
- Large Word and email sets are expected to take longer.
- Watch GUI live log cadence and run logs for progress markers.
- Confirm endpoint has adequate disk space and Word automation availability.

---

## Appendix: Operational Checklist for Support

When triaging incidents, request:
- Input path type used (folder or ZIP).
- Output root path.
- `processed/merge_manifest.json`.
- `logs/run_<id>.log`.
- Screenshot of GUI summary if available.

This is typically sufficient to determine:
- what was processed,
- what was relocated unprocessed,
- what failed and why,
- whether behavior is configuration-, input-, or environment-related.
