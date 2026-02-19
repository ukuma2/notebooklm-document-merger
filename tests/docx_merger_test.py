from pathlib import Path

from docx import Document

from merger_engine import DOCXMerger


def test_merge_valid_docx_creates_output(tmp_path, make_docx):
    doc1 = make_docx("a.docx", "Hello A")
    doc2 = make_docx("b.docx", "Hello B")
    output_dir = tmp_path / "out"

    merger = DOCXMerger(max_file_size_kb=1024)
    output_files = merger.merge_docx(
        [str(doc1), str(doc2)],
        str(output_dir),
        "group",
        warnings=[],
    )

    assert len(output_files) == 1
    merged_doc = Document(output_files[0])
    text = "\n".join(paragraph.text for paragraph in merged_doc.paragraphs)
    assert "Document: a.docx" in text
    assert "Document: b.docx" in text


def test_invalid_docx_is_skipped_with_warning(tmp_path):
    invalid = tmp_path / "bad.docx"
    invalid.write_text("invalid docx bytes", encoding="utf-8")
    output_dir = tmp_path / "out"
    warnings = []

    merger = DOCXMerger(max_file_size_kb=1024)
    output_files = merger.merge_docx(
        [str(invalid)],
        str(output_dir),
        "group",
        warnings=warnings,
    )

    assert output_files == []
    warning_codes = {warning["code"] for warning in warnings}
    assert "docx_unreadable" in warning_codes
    assert "docx_empty_batch" in warning_codes


def test_mixed_valid_and_invalid_docx_still_creates_output(tmp_path, make_docx):
    valid = make_docx("valid.docx", "Valid body")
    invalid = tmp_path / "invalid.docx"
    invalid.write_text("broken", encoding="utf-8")
    output_dir = tmp_path / "out"
    warnings = []

    merger = DOCXMerger(max_file_size_kb=1024)
    output_files = merger.merge_docx(
        [str(valid), str(invalid)],
        str(output_dir),
        "group",
        warnings=warnings,
    )

    assert len(output_files) == 1
    assert Path(output_files[0]).exists()
    assert any(warning["code"] == "docx_unreadable" for warning in warnings)


def test_docx_batch_size_splitting_behavior(monkeypatch, tmp_path, make_docx):
    doc_files = [make_docx(f"{idx}.docx", f"Doc {idx}") for idx in range(3)]
    output_dir = tmp_path / "out"

    monkeypatch.setattr("merger_engine.os.path.getsize", lambda *_args, **_kwargs: 700)

    merger = DOCXMerger(max_file_size_kb=1)  # 1024 bytes
    estimated = merger.estimate_batch_count([str(path) for path in doc_files])
    output_files = merger.merge_docx(
        [str(path) for path in doc_files],
        str(output_dir),
        "group",
        warnings=[],
    )

    assert estimated == 3
    assert len(output_files) == 3
