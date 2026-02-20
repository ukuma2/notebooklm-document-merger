"""
Local smoke test utility for validating ZIP preprocessing on real email exports.

This is intentionally not a pytest module to avoid running in CI by default.
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from merger_engine import MergeOrchestrator


def main() -> int:
    parser = argparse.ArgumentParser(description="Run real-data ZIP smoke validation.")
    parser.add_argument(
        "--zip",
        default="Emails-49431.zip",
        help="Path to the source ZIP archive.",
    )
    parser.add_argument(
        "--workdir",
        default="._tmp_zip_real_smoke",
        help="Workspace directory for temporary input/output.",
    )
    args = parser.parse_args()

    zip_path = Path(args.zip).resolve()
    if not zip_path.exists():
        raise SystemExit(f"ZIP not found: {zip_path}")

    workdir = Path(args.workdir).resolve()
    input_dir = workdir / "input"
    output_dir = workdir / "output"

    if workdir.exists():
        shutil.rmtree(workdir, ignore_errors=True)
    input_dir.mkdir(parents=True, exist_ok=True)

    copied_zip = input_dir / zip_path.name
    shutil.copy2(zip_path, copied_zip)

    orchestrator = MergeOrchestrator(
        process_pdfs=False,
        process_docx=False,
        process_emails=True,
    )
    result = orchestrator.merge_documents(str(input_dir), str(output_dir))

    summary = {
        "input_zip": str(copied_zip),
        "total_input_files": result.get("total_input_files"),
        "total_output_files": result.get("total_output_files"),
        "zip_processing": result.get("zip_processing", {}),
        "warning_codes": sorted({w.get("code") for w in result.get("warnings", [])}),
        "manifest_path": str(output_dir / "processed" / "merge_manifest.json"),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
