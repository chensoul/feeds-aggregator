from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import random
from time import monotonic

from .aggregator import AggregationConfig
from .errors import InputValidationError
from .failure_log import write_failure_log
from .input_loader import load_sources
from .models import AggregationResult, ProcessedOutput
from .output_writer import write_output_file
from .processing import ProcessingConfig, build_processed_output
from .reporting import TaskReport, build_task_report
from .runner import process_sources_to_items


@dataclass(slots=True, frozen=True)
class RunAggregationRequest:
    sources_path: str
    output_path: str
    workers: int
    timeout_seconds: float
    max_items_per_source: int
    max_total_items: int
    max_days: int
    timezone_name: str
    avatar_delay_ms: int = 200
    avatar_dir: str | None = None
    failure_log_path: str | None = None
    validate_only: bool = False


@dataclass(slots=True, frozen=True)
class RunAggregationResult:
    report: TaskReport
    processed: ProcessedOutput
    output_path: str | None
    aggregation: AggregationResult
    output_error: str | None = None
    failure_log_path: str | None = None
    failure_log_error: str | None = None
    validated_only: bool = False


def run_aggregation(request: RunAggregationRequest) -> RunAggregationResult:
    started_at = monotonic()
    input_result = load_sources(request.sources_path)
    sources = shuffle_sources(input_result.sources)
    if request.validate_only:
        processed = ProcessedOutput(items=[], updated="")
        duration_seconds = monotonic() - started_at
        report = TaskReport(
            outcome="success",
            total_sources=len(sources),
            successful_sources=0,
            failed_sources=0,
            output_items=0,
            downloaded_avatars=0,
            duration_seconds=duration_seconds,
            failures=[],
        )
        return RunAggregationResult(
            report=report,
            processed=processed,
            output_path=None,
            aggregation=AggregationResult(),
            failure_log_path=None,
            validated_only=True,
        )

    now = datetime.now(UTC)
    processing_config = ProcessingConfig(
        max_items_per_source=request.max_items_per_source,
        max_total_items=request.max_total_items,
        max_days=request.max_days,
        timezone_name=request.timezone_name,
        now=now,
    )
    aggregation, source_items = process_sources_to_items(
        sources,
        aggregation_config=AggregationConfig(timeout_seconds=request.timeout_seconds, workers=request.workers),
        processing_config=processing_config,
        output_path=request.output_path,
        avatar_dir=request.avatar_dir,
        avatar_delay_ms=request.avatar_delay_ms,
    )
    processed = build_processed_output(source_items, config=processing_config, now=now)

    written_output_path: str | None = None
    output_written = False
    output_error: str | None = None
    if aggregation.outcome != "failure":
        try:
            written_output_path = str(write_output_file(processed, request.output_path))
            output_written = True
        except OSError as exc:
            output_written = False
            output_error = str(exc)

    written_failure_log_path: str | None = None
    failure_log_error: str | None = None
    if request.failure_log_path and aggregation.failures:
        try:
            written_failure_log_path = str(write_failure_log(aggregation.failures, request.failure_log_path))
        except OSError as exc:
            failure_log_error = str(exc)

    report = build_task_report(
        aggregation,
        processed,
        output_written=output_written,
        duration_seconds=monotonic() - started_at,
    )
    return RunAggregationResult(
        report=report,
        processed=processed,
        output_path=written_output_path,
        aggregation=aggregation,
        output_error=output_error,
        failure_log_path=written_failure_log_path,
        failure_log_error=failure_log_error,
    )


def shuffle_sources(sources: list) -> list:
    shuffled = list(sources)
    random.shuffle(shuffled)
    return shuffled
