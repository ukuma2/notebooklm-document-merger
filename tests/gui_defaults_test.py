from pathlib import Path


def test_gui_default_max_file_size_is_100mb():
    source = Path("document_merger_gui.py").read_text(encoding="utf-8")
    assert "self.max_file_size = tk.IntVar(value=102400)" in source
