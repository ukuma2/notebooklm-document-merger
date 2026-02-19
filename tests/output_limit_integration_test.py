import pytest

from merger_engine import MergeOrchestrator


def test_orchestrator_enforces_max_output_files_across_types(tmp_path, make_pdf, make_docx, make_eml, patch_word_converter):
    patch_word_converter()
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    pdf_file = make_pdf("one.pdf", pages=1)
    docx_file = make_docx("one.docx", "Body")
    eml_file = make_eml("one.eml", "Subject", "Body")

    (input_dir / pdf_file.name).write_bytes(pdf_file.read_bytes())
    (input_dir / docx_file.name).write_bytes(docx_file.read_bytes())
    (input_dir / eml_file.name).write_text(eml_file.read_text(encoding="utf-8"), encoding="utf-8")

    orchestrator = MergeOrchestrator(
        max_file_size_kb=1024,
        max_output_files=2,
        process_pdfs=True,
        process_docx=True,
        process_emails=True,
    )

    with pytest.raises(RuntimeError, match="max_output_files"):
        orchestrator.merge_documents(str(input_dir), str(tmp_path / "out"))
