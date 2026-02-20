from pathlib import Path

from pypdf import PdfReader

from merger_engine import MergeOrchestrator


def _flatten_outline_titles(outline):
    titles = []

    def walk(items):
        for item in items:
            if isinstance(item, list):
                walk(item)
                continue
            title = getattr(item, "title", None)
            if title:
                titles.append(str(title))

    if isinstance(outline, list):
        walk(outline)
    return titles


def test_word_documents_convert_and_merge_to_pdf_with_mapping(
    tmp_path,
    make_docx,
    patch_word_converter,
):
    patch_word_converter()
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    docx_file = make_docx("a.docx", "A body")
    doc_file = tmp_path / "b.doc"
    doc_file.write_text("legacy doc placeholder", encoding="utf-8")

    (input_dir / docx_file.name).write_bytes(docx_file.read_bytes())
    (input_dir / doc_file.name).write_bytes(doc_file.read_bytes())

    orchestrator = MergeOrchestrator(
        max_file_size_kb=1024,
        process_pdfs=False,
        process_docx=True,
        process_emails=False,
    )
    result = orchestrator.merge_documents(str(input_dir), str(tmp_path / "out"))

    assert result["total_output_files"] == 1
    output_pdf = Path(result["output_files"][0])
    assert output_pdf.name == "root_documents_batch1.pdf"
    assert output_pdf.exists()

    merged_reader = PdfReader(str(output_pdf))
    assert len(merged_reader.pages) == 2

    assert result["word_conversion"] == {"attempted": 2, "converted": 2, "failed": 0}
    mapped_sources = result["output_to_sources"][str(output_pdf)]
    assert sorted(mapped_sources) == sorted([str(input_dir / "a.docx"), str(input_dir / "b.doc")])


def test_word_conversion_failures_are_skipped_with_warning(
    tmp_path,
    make_docx,
    patch_word_converter,
):
    patch_word_converter(fail_contains="bad")
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    good = make_docx("good.docx", "ok")
    bad = make_docx("bad.docx", "broken")
    (input_dir / good.name).write_bytes(good.read_bytes())
    (input_dir / bad.name).write_bytes(bad.read_bytes())

    orchestrator = MergeOrchestrator(
        max_file_size_kb=1024,
        process_pdfs=False,
        process_docx=True,
        process_emails=False,
    )
    result = orchestrator.merge_documents(str(input_dir), str(tmp_path / "out"))

    assert result["total_output_files"] == 1
    assert result["word_conversion"] == {"attempted": 2, "converted": 1, "failed": 1}
    warning_codes = {warning["code"] for warning in result.get("warnings", [])}
    assert "word_to_pdf_failed" in warning_codes


def test_word_output_includes_source_bookmarks(tmp_path, make_docx, patch_word_converter):
    patch_word_converter()
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    first = make_docx("first.docx", "first")
    second = make_docx("second.docx", "second")
    (input_dir / first.name).write_bytes(first.read_bytes())
    (input_dir / second.name).write_bytes(second.read_bytes())

    orchestrator = MergeOrchestrator(
        max_file_size_kb=1024,
        process_pdfs=False,
        process_docx=True,
        process_emails=False,
    )
    result = orchestrator.merge_documents(str(input_dir), str(tmp_path / "out"))
    output_pdf = result["output_files"][0]

    reader = PdfReader(output_pdf)
    titles = _flatten_outline_titles(reader.outline)

    assert "first.docx" in titles
    assert "second.docx" in titles


def test_word_batch_size_splitting_honors_user_value(
    monkeypatch,
    tmp_path,
    make_docx,
    patch_word_converter,
):
    patch_word_converter()
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    files = [make_docx(f"{idx}.docx", f"doc {idx}") for idx in range(3)]
    for file_path in files:
        (input_dir / file_path.name).write_bytes(file_path.read_bytes())

    monkeypatch.setattr("merger_engine.os.path.getsize", lambda *_args, **_kwargs: 700)

    orchestrator = MergeOrchestrator(
        max_file_size_kb=1,  # 1024 bytes
        process_pdfs=False,
        process_docx=True,
        process_emails=False,
    )
    result = orchestrator.merge_documents(str(input_dir), str(tmp_path / "out"))

    assert result["total_output_files"] == 3
    assert all(Path(path).name.startswith("root_documents_batch") for path in result["output_files"])


def test_word_progress_logging_emits_interval_updates(
    tmp_path,
    make_docx,
    patch_word_converter,
    capsys,
):
    patch_word_converter()
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    files = [make_docx(f"{idx}.docx", f"Doc {idx}") for idx in range(5)]
    for file_path in files:
        (input_dir / file_path.name).write_bytes(file_path.read_bytes())

    orchestrator = MergeOrchestrator(
        max_file_size_kb=1024,
        process_pdfs=False,
        process_docx=True,
        process_emails=False,
        word_progress_interval=2,
    )
    orchestrator.merge_documents(str(input_dir), str(tmp_path / "out"))

    output = capsys.readouterr().out
    assert "Word conversion progress for root: 2/5" in output
    assert "Word conversion progress for root: 4/5" in output
    assert "Word conversion progress for root: 5/5" in output
    assert "Word conversion summary for root: attempted=5, converted=5, failed=0" in output
