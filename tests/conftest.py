from pathlib import Path
import shutil
import os
import tempfile
import uuid

import pytest
from docx import Document
from pypdf import PdfWriter


@pytest.fixture
def tmp_path():
    """
    Local override for pytest's tmp_path fixture.
    Some Windows environments create tmp roots with restrictive ACLs that
    break test setup/teardown. This keeps temp dirs under LOCALAPPDATA/Temp.
    """
    base_root = Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir()))
    base = base_root / "Temp" / "codex_pytest_cases"
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"case_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


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


@pytest.fixture
def patch_word_converter(monkeypatch):
    def _patch(fail_contains: str = ""):
        class FakeWordConverter:
            def __init__(self, warnings=None):
                self.warnings = warnings

            @staticmethod
            def is_available():
                return True, ""

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def convert_file(self, source_path: str, output_pdf_path: str) -> bool:
                if fail_contains and fail_contains in source_path:
                    if self.warnings is not None:
                        self.warnings.append(
                            {
                                "code": "word_to_pdf_failed",
                                "message": "Word-to-PDF conversion failed; skipping file",
                                "file": source_path,
                                "error": "mock_failure",
                            }
                        )
                    return False

                writer = PdfWriter()
                writer.add_blank_page(width=72, height=72)
                with open(output_pdf_path, "wb") as handle:
                    writer.write(handle)
                return True

        monkeypatch.setattr("merger_engine.WordToPdfConverter", FakeWordConverter)

    return _patch
