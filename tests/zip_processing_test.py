import io
import zipfile
from pathlib import Path

from merger_engine import MergeOrchestrator


def _build_eml(subject: str, body: str, date: str = "Mon, 1 Jan 2024 10:00:00 +0000") -> str:
    return (
        "From: sender@example.com\n"
        "To: receiver@example.com\n"
        f"Subject: {subject}\n"
        f"Date: {date}\n"
        "Content-Type: text/plain; charset=utf-8\n"
        "\n"
        f"{body}\n"
    )


def _write_zip(zip_path: Path, entries):
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries:
            archive.writestr(name, payload)


def _run_email_only(input_dir: Path, output_dir: Path, **kwargs):
    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
        **kwargs,
    )
    return orchestrator.merge_documents(str(input_dir), str(output_dir))


def test_zip_with_long_email_names_extracts_and_threads(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    long_name = ("x" * 120) + ".eml"
    zip_path = input_dir / "emails.zip"
    _write_zip(zip_path, [(long_name, _build_eml("Subject", "Body from long filename"))])

    result = _run_email_only(input_dir, output_dir)

    assert result["total_output_files"] == 1
    thread_file = Path(result["output_files"][0])
    assert thread_file.name.startswith("root_emails_emails_batch")
    assert "Body from long filename" in thread_file.read_text(encoding="utf-8")
    assert result["zip_processing"]["entries_renamed"] > 0


def test_zip_truncation_collision_resolves_uniquely(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    repeated = "a" * 80
    first_name = f"{repeated}_first.eml"
    second_name = f"{repeated}_second.eml"

    zip_path = input_dir / "collision.zip"
    _write_zip(
        zip_path,
        [
            (first_name, _build_eml("Collision", "first body")),
            (second_name, _build_eml("Collision", "second body")),
        ],
    )

    result = _run_email_only(input_dir, output_dir)

    assert result["total_output_files"] == 1
    text = Path(result["output_files"][0]).read_text(encoding="utf-8")
    source_lines = [
        line.replace("Source: ", "", 1)
        for line in text.splitlines()
        if line.startswith("Source: ")
    ]
    assert len(source_lines) == 2
    assert len(set(source_lines)) == 2
    assert all(len(name) <= 50 for name in source_lines)
    assert "first body" in text
    assert "second body" in text


def test_nested_zip_one_level_supported(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    inner_buffer = io.BytesIO()
    with zipfile.ZipFile(inner_buffer, "w", compression=zipfile.ZIP_DEFLATED) as inner_zip:
        inner_zip.writestr("inside.eml", _build_eml("Nested", "From nested zip"))

    outer_zip = input_dir / "outer.zip"
    _write_zip(outer_zip, [("inner.zip", inner_buffer.getvalue())])

    result = _run_email_only(input_dir, output_dir)

    assert result["total_output_files"] == 1
    text = Path(result["output_files"][0]).read_text(encoding="utf-8")
    assert "From nested zip" in text
    assert result["zip_processing"]["nested_archives_extracted"] >= 1


def test_nested_zip_deeper_than_limit_is_skipped_with_warning(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    deep_buffer = io.BytesIO()
    with zipfile.ZipFile(deep_buffer, "w", compression=zipfile.ZIP_DEFLATED) as deep_zip:
        deep_zip.writestr("deep.eml", _build_eml("Deep", "Should not be extracted"))

    middle_buffer = io.BytesIO()
    with zipfile.ZipFile(middle_buffer, "w", compression=zipfile.ZIP_DEFLATED) as middle_zip:
        middle_zip.writestr("deep.zip", deep_buffer.getvalue())

    outer_zip = input_dir / "outer.zip"
    _write_zip(outer_zip, [("middle.zip", middle_buffer.getvalue())])

    result = _run_email_only(input_dir, output_dir)

    assert result["total_output_files"] == 0
    warning_codes = {warning["code"] for warning in result.get("warnings", [])}
    assert "zip_nested_depth_exceeded" in warning_codes
    assert result["zip_processing"]["nested_archives_skipped_depth"] >= 1


def test_zip_slip_entry_is_blocked(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    archive_path = input_dir / "slip.zip"
    _write_zip(
        archive_path,
        [
            ("../evil.eml", _build_eml("Evil", "Should be skipped")),
            ("good.eml", _build_eml("Good", "Expected body")),
        ],
    )

    result = _run_email_only(input_dir, output_dir)

    warning_codes = {warning["code"] for warning in result.get("warnings", [])}
    assert "zip_entry_skipped_unsafe_path" in warning_codes
    assert result["zip_processing"]["entries_skipped_unsafe_path"] >= 1
    skipped_codes = {item["code"] for item in result["files"]["skipped"]}
    assert "zip_entry_skipped_unsafe_path" in skipped_codes
    assert not (tmp_path / "evil.eml").exists()
    assert result["total_output_files"] == 1
    assert "Expected body" in Path(result["output_files"][0]).read_text(encoding="utf-8")


def test_mixed_zip_and_plain_files_processed_together(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    plain_email = input_dir / "plain.eml"
    plain_email.write_text(_build_eml("Plain", "Plain body"), encoding="utf-8")

    archive_path = input_dir / "bundle.zip"
    _write_zip(archive_path, [("zipped.eml", _build_eml("Zipped", "Zipped body"))])

    result = _run_email_only(input_dir, output_dir)

    output_names = [Path(path).name for path in result["output_files"]]
    assert any(name.startswith("root_emails_batch") for name in output_names)
    assert any(name.startswith("root_bundle_emails_batch") for name in output_names)


def test_single_zip_file_path_input_supported(tmp_path):
    zip_path = tmp_path / "single.zip"
    _write_zip(zip_path, [("single.eml", _build_eml("Only", "single zip input body"))])
    output_dir = tmp_path / "output"

    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
    )
    result = orchestrator.merge_documents(str(zip_path), str(output_dir))

    assert result["total_input_files"] == 1
    assert result["total_output_files"] == 1
    assert result["zip_processing"]["archives_found"] == 1
    assert "single zip input body" in Path(result["output_files"][0]).read_text(encoding="utf-8")


def test_zip_unsupported_files_are_moved_to_unprocessed_folder(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    zip_path = input_dir / "mixed.zip"
    _write_zip(
        zip_path,
        [
            ("mail.eml", _build_eml("Mail", "mail body")),
            ("table.xlsx", b"fake excel bytes"),
            ("sub/facts.csv", "a,b\n1,2\n"),
        ],
    )

    result = _run_email_only(input_dir, output_dir)

    assert result["summary"]["moved_unprocessed_total"] == 2
    moved = result["files"]["moved_unprocessed"]
    assert len(moved) == 2
    assert all(item["reason"] == "unsupported_zip_file_moved" for item in moved)
    for item in moved:
        assert Path(item["destination"]).exists()
        assert str(item["destination"]).startswith(result["paths"]["unprocessed_dir"])
        assert Path(item["destination"]).parent == Path(result["paths"]["unprocessed_dir"])
    assert len(result["files"]["unprocessed"]) == 2

    processed_manifest = Path(result["paths"]["processed_dir"]) / "merge_manifest.json"
    assert processed_manifest.exists()
    assert Path(result["logs"]["text_log"]).exists()
    assert Path(result["logs"]["jsonl_log"]).exists()


def test_input_unsupported_files_are_copied_to_unprocessed_folder(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    (input_dir / "mail.eml").write_text(_build_eml("Subject", "Body"), encoding="utf-8")
    (input_dir / "meta.xml").write_text("<root/>", encoding="utf-8")
    (input_dir / "image.png").write_bytes(b"png")

    result = _run_email_only(input_dir, output_dir)

    assert result["summary"]["unprocessed_relocated_total"] == 2
    unprocessed = result["files"]["unprocessed"]
    assert len(unprocessed) == 2
    assert all(item["origin"] == "input" for item in unprocessed)
    assert all(item["action"] == "copy" for item in unprocessed)
    for item in unprocessed:
        assert Path(item["destination"]).exists()
        assert Path(item["destination"]).parent == Path(result["paths"]["unprocessed_dir"])
