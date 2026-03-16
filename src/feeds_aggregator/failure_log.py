from __future__ import annotations

import json
from pathlib import Path

from .models import SourceAggregationFailure


def write_failure_log(failures: list[SourceAggregationFailure], output_path: str | Path) -> Path:
    path = Path(output_path)
    if path.exists() and path.is_dir():
        raise OSError(f"Failure log path is a directory: {path}")

    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "source_url": failure.source.source_url,
            "category": failure.source.category,
            "source_name": failure.source.source_name,
            "error": failure.error,
        }
        for failure in failures
    ]
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
