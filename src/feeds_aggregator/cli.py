from __future__ import annotations

import argparse
import json
import logging
import sys

from .application import RunAggregationRequest, run_aggregation
from .errors import InputValidationError
from .processing import DEFAULT_MAX_DAYS, DEFAULT_MAX_ITEMS_PER_SOURCE, DEFAULT_MAX_TOTAL_ITEMS, DEFAULT_TIMEZONE

SUCCESS_EXIT_CODE = 0
FAILURE_EXIT_CODE = 1
INPUT_ERROR_EXIT_CODE = 2
LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_MESSAGE_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
logger = logging.getLogger(__name__)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="feeds-aggregator",
        description="Load and aggregate feed sources for Feeds Aggregator.",
    )
    parser.add_argument("--sources", required=True, help="Path to the feed source input file")
    parser.add_argument("--output", default="data/feeds.json", help="Path to the output result file")
    parser.add_argument("--workers", type=positive_int, default=8, help="Concurrent source workers")
    parser.add_argument("--timeout", type=positive_float, default=15.0, help="Per-source request timeout in seconds")
    parser.add_argument("--avatar-delay-ms", type=non_negative_int, default=200, help="Delay between avatar discovery/download requests in milliseconds")
    parser.add_argument(
        "--max-items-per-source",
        type=non_negative_int,
        default=DEFAULT_MAX_ITEMS_PER_SOURCE,
        help=f"Max items per source (default: {DEFAULT_MAX_ITEMS_PER_SOURCE})",
    )
    parser.add_argument(
        "--max-total-items",
        type=non_negative_int,
        default=DEFAULT_MAX_TOTAL_ITEMS,
        help=f"Max total items (default: {DEFAULT_MAX_TOTAL_ITEMS})",
    )
    parser.add_argument(
        "--max-days",
        type=non_negative_int,
        default=DEFAULT_MAX_DAYS,
        help=f"Keep only items from recent days (default: {DEFAULT_MAX_DAYS})",
    )
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE, help=f"IANA timezone for output timestamps (default: {DEFAULT_TIMEZONE})")
    parser.add_argument("--avatar-dir", default="", help="Directory to store downloaded avatar images (default: <output-dir>/avatars)")
    parser.add_argument("--failure-log", default="", help="Optional JSON file path for writing failed feed details")
    parser.add_argument("--validate-only", action="store_true", help="Validate inputs and configuration without fetching feeds or writing output")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging()
    logger.info("Starting feeds aggregation run")

    try:
        result = run_aggregation(
            RunAggregationRequest(
                sources_path=args.sources,
                output_path=args.output,
                workers=args.workers,
                timeout_seconds=args.timeout,
                avatar_delay_ms=args.avatar_delay_ms,
                max_items_per_source=args.max_items_per_source,
                max_total_items=args.max_total_items,
                max_days=args.max_days,
                timezone_name=args.timezone,
                avatar_dir=args.avatar_dir or None,
                failure_log_path=args.failure_log or None,
                validate_only=args.validate_only,
            )
        )
    except InputValidationError as exc:
        print(f"input error: {exc}", file=sys.stderr)
        return INPUT_ERROR_EXIT_CODE
    except Exception as exc:
        print(f"runtime error: {exc}", file=sys.stderr)
        return FAILURE_EXIT_CODE

    logger.info(
        "Aggregation result: %d successes, %d failures",
        len(result.aggregation.successes),
        len(result.aggregation.failures),
    )
    if result.output_path is not None:
        logger.info("Output written to %s", result.output_path)
    if result.failure_log_path is not None:
        logger.info("Failure log written to %s", result.failure_log_path)
    logger.info(
        "Run finished with outcome=%s, sources=%d, failures=%d, items=%d, avatars=%d, duration=%.3fs",
        result.report.outcome,
        result.report.total_sources,
        result.report.failed_sources,
        result.report.output_items,
        result.report.downloaded_avatars,
        result.report.duration_seconds,
    )

    print(
        json.dumps(
            build_summary_payload(
                report=result.report,
                output_path=result.output_path,
                failure_log_path=result.failure_log_path,
                validated_only=result.validated_only,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    if result.output_error is not None:
        print(f"output error: {result.output_error}", file=sys.stderr)
    if result.failure_log_error is not None:
        print(f"failure log error: {result.failure_log_error}", file=sys.stderr)
    for failure in result.report.failures:
        print(f"source failed: {failure.source.source_url} -> {failure.error}", file=sys.stderr)

    if result.report.outcome == "failure":
        return FAILURE_EXIT_CODE
    return SUCCESS_EXIT_CODE


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_MESSAGE_FORMAT,
        datefmt=LOG_TIME_FORMAT,
    )


def build_summary_payload(
    *,
    report,
    output_path: str | None,
    failure_log_path: str | None = None,
    validated_only: bool = False,
) -> dict[str, object]:
    return {
        "outcome": report.outcome,
        "total_sources": report.total_sources,
        "successful_sources": report.successful_sources,
        "failed_sources": report.failed_sources,
        "output_items": report.output_items,
        "downloaded_avatars": report.downloaded_avatars,
        "duration_seconds": round(report.duration_seconds, 3),
        "output_path": output_path,
        "failure_log_path": failure_log_path,
        "validated_only": validated_only,
        "failed_feed_urls": [failure.source.source_url for failure in report.failures],
    }


if __name__ == "__main__":
    raise SystemExit(main())
