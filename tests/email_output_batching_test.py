from email.message import EmailMessage
from pathlib import Path

from merger_engine import MergeOrchestrator


def _build_eml(subject: str, body: str) -> str:
    return (
        "From: sender@example.com\n"
        "To: receiver@example.com\n"
        f"Subject: {subject}\n"
        "Date: Mon, 1 Jan 2024 10:00:00 +0000\n"
        "Content-Type: text/plain; charset=utf-8\n"
        "\n"
        f"{body}\n"
    )


def test_email_size_batching_respects_limit(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    for index in range(24):
        body = ("line " + str(index) + "\n") * 18000
        (input_dir / f"m{index}.eml").write_text(
            _build_eml(f"Thread {index}", body),
            encoding="utf-8",
        )

    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
        email_max_output_file_mb=1,
    )
    result = orchestrator.merge_documents(str(input_dir), str(output_dir))

    outputs = [Path(path) for path in result["output_files"]]
    assert len(outputs) >= 2
    assert all(path.name.startswith("root_emails_batch") for path in outputs)
    assert all(path.stat().st_size <= (1 * 1024 * 1024 + 128 * 1024) for path in outputs)
    assert result["emails"]["parsed_total"] == 24
    assert result["emails"]["batches_total"] == len(outputs)


def test_email_output_contains_attachment_index(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    msg = EmailMessage()
    msg["From"] = "sender@example.com"
    msg["To"] = "receiver@example.com"
    msg["Subject"] = "Attachment Subject"
    msg.set_content("Body text")
    msg.add_attachment(
        b"binary-content",
        maintype="application",
        subtype="octet-stream",
        filename="attachment.bin",
    )
    (input_dir / "with_attachment.eml").write_bytes(msg.as_bytes())

    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
    )
    result = orchestrator.merge_documents(str(input_dir), str(output_dir))

    output_text = Path(result["output_files"][0]).read_text(encoding="utf-8")
    assert "ATTACHMENTS:" in output_text
    assert "attachment.bin" in output_text


def test_failed_email_is_copied_to_failed_folder(tmp_path, monkeypatch):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    broken = input_dir / "broken.eml"
    broken.write_text(_build_eml("Broken", "Will fail"), encoding="utf-8")

    monkeypatch.setattr(
        "merger_engine.EmailExtractor.extract_eml",
        staticmethod(lambda _path: None),
    )

    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
    )
    result = orchestrator.merge_documents(str(input_dir), str(output_dir))

    failed_items = result["files"]["failed"]
    assert failed_items
    item = failed_items[0]
    assert item["artifact_status"] == "created"
    assert Path(item["artifact_destination"]).exists()
    assert str(item["artifact_destination"]).startswith(result["paths"]["failed_dir"])
