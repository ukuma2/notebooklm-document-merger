from pathlib import Path

from PIL import Image
from pypdf import PdfReader

from merger_engine import PDFMerger


def test_merge_valid_pdfs_creates_output(tmp_path, make_pdf):
    pdf1 = make_pdf("a.pdf", pages=1)
    pdf2 = make_pdf("b.pdf", pages=2)
    output_dir = tmp_path / "out"

    merger = PDFMerger(max_file_size_kb=1024)
    output_files = merger.merge_pdfs(
        [str(pdf1), str(pdf2)],
        str(output_dir),
        "case",
        warnings=[],
    )

    assert len(output_files) == 1
    merged_reader = PdfReader(output_files[0])
    assert len(merged_reader.pages) == 3


def test_corrupt_pdf_is_skipped_with_warning_and_empty_batch_not_written(tmp_path):
    corrupt_pdf = tmp_path / "broken.pdf"
    corrupt_pdf.write_text("not a pdf", encoding="utf-8")
    output_dir = tmp_path / "out"
    warnings = []

    merger = PDFMerger(max_file_size_kb=1024)
    output_files = merger.merge_pdfs(
        [str(corrupt_pdf)],
        str(output_dir),
        "case",
        warnings=warnings,
    )

    assert output_files == []
    warning_codes = {warning["code"] for warning in warnings}
    assert "pdf_unreadable" in warning_codes or "pdf_conversion_failed" in warning_codes
    assert "pdf_empty_batch" in warning_codes


def test_image_fallback_conversion_creates_output(tmp_path):
    disguised_image = tmp_path / "scan.pdf"
    image = Image.new("RGB", (16, 16), color="white")
    image.save(disguised_image, format="PNG")

    output_dir = tmp_path / "out"
    warnings = []

    merger = PDFMerger(max_file_size_kb=1024)
    output_files = merger.merge_pdfs(
        [str(disguised_image)],
        str(output_dir),
        "case",
        warnings=warnings,
    )

    assert len(output_files) == 1
    assert Path(output_files[0]).exists()


def test_pdf_batch_size_splitting_behavior(monkeypatch, tmp_path, make_pdf):
    pdf_files = [make_pdf(f"{idx}.pdf", pages=1) for idx in range(3)]
    output_dir = tmp_path / "out"

    monkeypatch.setattr("merger_engine.os.path.getsize", lambda *_args, **_kwargs: 700)

    merger = PDFMerger(max_file_size_kb=1)  # 1024 bytes
    estimated = merger.estimate_batch_count([str(path) for path in pdf_files])
    output_files = merger.merge_pdfs(
        [str(path) for path in pdf_files],
        str(output_dir),
        "case",
        warnings=[],
    )

    assert estimated == 3
    assert len(output_files) == 3
