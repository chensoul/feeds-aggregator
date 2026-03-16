from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .models import AggregationResult, ProcessedOutput, SourceAggregationFailure

TaskOutcome = Literal["success", "partial_success", "failure"]


@dataclass(slots=True, frozen=True)
class TaskReport:
    outcome: TaskOutcome
    total_sources: int
    successful_sources: int
    failed_sources: int
    output_items: int
    downloaded_avatars: int
    duration_seconds: float
    failures: list[SourceAggregationFailure]


def build_task_report(
    aggregation: AggregationResult,
    processed: ProcessedOutput,
    *,
    output_written: bool,
    duration_seconds: float,
) -> TaskReport:
    if not output_written or aggregation.outcome == "failure":
        outcome: TaskOutcome = "failure"
    elif aggregation.failures:
        outcome = "partial_success"
    else:
        outcome = "success"

    return TaskReport(
        outcome=outcome,
        total_sources=aggregation.total_sources,
        successful_sources=len(aggregation.successes),
        failed_sources=len(aggregation.failures),
        output_items=len(processed.items),
        downloaded_avatars=count_downloaded_avatars(processed),
        duration_seconds=duration_seconds,
        failures=aggregation.failures,
    )


def count_downloaded_avatars(processed: ProcessedOutput) -> int:
    local_avatars = {
        item.avatar
        for item in processed.items
        if item.avatar and not str(item.avatar).startswith(("http://", "https://"))
    }
    return len(local_avatars)
