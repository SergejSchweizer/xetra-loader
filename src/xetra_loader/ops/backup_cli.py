"""Verify a full-sync recovery backup without connecting to PostgreSQL."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from xetra_loader.logging_config import configure_logging
from xetra_loader.ops.verify_postgres_sync import (
    ProductionVerificationError,
    verify_backup_checksums,
)


def main(argv: Sequence[str] | None = None) -> int:
    """Validate every file and the exact file inventory in a backup directory."""

    configure_logging("xdl-verify-backup")
    parser = argparse.ArgumentParser(prog="xdl-verify-backup")
    parser.add_argument("backup_directory", type=Path)
    arguments = parser.parse_args(argv)
    try:
        verified_files = verify_backup_checksums(arguments.backup_directory)
    except (OSError, ProductionVerificationError):
        print(json.dumps({"status": "FAIL", "error": "backup_verification_failed"}))
        return 1
    print(json.dumps({"status": "PASS", "verified_files": verified_files}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
