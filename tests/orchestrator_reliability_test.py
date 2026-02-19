from merger_engine import MergeOrchestrator


def test_email_only_creates_output_directory_and_manifest(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    email_file = input_dir / "thread.eml"
    email_file.write_text(
        "From: a@example.com\n"
        "To: b@example.com\n"
        "Subject: Thread\n"
        "Date: Mon, 1 Jan 2024 10:00:00 +0000\n"
        "Content-Type: text/plain; charset=utf-8\n\n"
        "Hello\n",
        encoding="utf-8",
    )

    output_dir = input_dir / "merged_output"

    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
    )
    result = orchestrator.merge_documents(str(input_dir), str(output_dir))

    assert result["total_input_files"] == 1
    assert result["total_output_files"] == 1
    assert output_dir.exists()
    assert (output_dir / "merge_manifest.json").exists()
    assert "limits" in result
    assert result["limits"]["max_output_files"] == 300


def test_output_directory_nested_in_input_is_excluded_from_scan(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = input_dir / "merged_output"
    input_dir.mkdir()
    output_dir.mkdir(parents=True)

    (input_dir / "source.eml").write_text(
        "From: a@example.com\nTo: b@example.com\nSubject: Main\n"
        "Content-Type: text/plain; charset=utf-8\n\nBody\n",
        encoding="utf-8",
    )
    (output_dir / "already_generated.eml").write_text(
        "From: x@example.com\nTo: y@example.com\nSubject: Generated\n"
        "Content-Type: text/plain; charset=utf-8\n\nGenerated\n",
        encoding="utf-8",
    )

    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
    )
    result = orchestrator.merge_documents(str(input_dir), str(output_dir))

    # Only the source input email should be counted.
    assert result["total_input_files"] == 1


def test_progress_callback_uses_global_total(tmp_path):
    input_dir = tmp_path / "input"
    group_a = input_dir / "GroupA"
    group_b = input_dir / "GroupB"
    group_a.mkdir(parents=True)
    group_b.mkdir(parents=True)

    (group_a / "a.eml").write_text(
        "From: a@example.com\nTo: b@example.com\nSubject: A\n"
        "Content-Type: text/plain; charset=utf-8\n\nBody A\n",
        encoding="utf-8",
    )
    (group_b / "b.eml").write_text(
        "From: a@example.com\nTo: b@example.com\nSubject: B\n"
        "Content-Type: text/plain; charset=utf-8\n\nBody B\n",
        encoding="utf-8",
    )

    callbacks = []

    def progress(current, total, message):
        callbacks.append((current, total, message))

    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
    )
    result = orchestrator.merge_documents(
        str(input_dir),
        str(tmp_path / "output"),
        progress_callback=progress,
    )

    assert result["total_input_files"] == 2
    assert callbacks
    assert all(total == 2 for _, total, _ in callbacks)
    assert callbacks[-1][0] == 2
