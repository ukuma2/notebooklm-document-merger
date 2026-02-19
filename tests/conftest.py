from pathlib import Path

import pytest
from docx import Document
from pypdf import PdfWriter


@pytest.fixture
def io_dirs(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir


@pytest.fixture
def make_pdf(tmp_path: Path):
    def _make(filename: str, pages: int = 1) -> Path:
        path = tmp_path / filename
        writer = PdfWriter()
        for _ in range(pages):
            writer.add_blank_page(width=72, height=72)
        with path.open("wb") as handle:
            writer.write(handle)
        return path

    return _make


@pytest.fixture
def make_docx(tmp_path: Path):
    def _make(filename: str, text: str) -> Path:
        path = tmp_path / filename
        document = Document()
        document.add_paragraph(text)
        document.save(path)
        return path

    return _make


@pytest.fixture
def make_eml(tmp_path: Path):
    def _make(filename: str, subject: str, body: str, date_header: str = "") -> Path:
        path = tmp_path / filename
        date_line = f"Date: {date_header}\n" if date_header else ""
        content = (
            "From: sender@example.com\n"
            "To: receiver@example.com\n"
            f"Subject: {subject}\n"
            f"{date_line}"
            "Content-Type: text/plain; charset=utf-8\n"
            "\n"
            f"{body}\n"
        )
        path.write_text(content, encoding="utf-8")
        return path

    return _make
