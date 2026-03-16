from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime

from .aggregator import AggregationConfig, fetch_and_parse_source, resolve_worker_count
from .models import AggregationResult, FeedSource, ProcessedItem, RawFeedDocument, SourceAggregationFailure
from .output_writer import persist_item_avatars
from .processing import ProcessingConfig, process_document, should_stop_after_limit

SourceProcessingResult = tuple[RawFeedDocument, list[ProcessedItem]]


def process_sources_to_items(
    sources: list[FeedSource],
    *,
    aggregation_config: AggregationConfig,
    processing_config: ProcessingConfig,
    output_path: str,
    avatar_dir: str | None,
) -> tuple[AggregationResult, list[ProcessedItem]]:
    if not sources:
        return AggregationResult(), []

    workers = resolve_worker_count(aggregation_config.workers, len(sources))
    successes: list[RawFeedDocument] = []
    failures: list[SourceAggregationFailure] = []
    source_items: list[ProcessedItem] = []
    pending_sources = iter(sources)
    stop_submitting = False

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_source: dict[Future[SourceProcessingResult], FeedSource] = {}

        def submit_next_source() -> bool:
            try:
                source = next(pending_sources)
            except StopIteration:
                return False
            future: Future[SourceProcessingResult] = executor.submit(
                process_single_source,
                source,
                aggregation_config=aggregation_config,
                processing_config=processing_config,
                output_path=output_path,
                avatar_dir=avatar_dir,
            )
            future_to_source[future] = source
            return True

        for _ in range(workers):
            if not submit_next_source():
                break

        while future_to_source:
            done, _ = wait(set(future_to_source), return_when=FIRST_COMPLETED)
            for future in done:
                source = future_to_source.pop(future)
                try:
                    document, items = future.result()
                except Exception as exc:
                    failures.append(SourceAggregationFailure(source=source, error=str(exc)))
                else:
                    successes.append(document)
                    source_items.extend(items)
                    if has_reached_total_limit(source_items, processing_config):
                        stop_submitting = True

                if not stop_submitting:
                    submit_next_source()

        if stop_submitting:
            executor.shutdown(wait=False, cancel_futures=True)

    return AggregationResult(successes=successes, failures=failures), source_items


def process_single_source(
    source: FeedSource,
    *,
    aggregation_config: AggregationConfig,
    processing_config: ProcessingConfig,
    output_path: str,
    avatar_dir: str | None,
) -> SourceProcessingResult:
    document = fetch_and_parse_source(source, aggregation_config)
    now = processing_config.now or datetime.now(UTC)
    items = process_document(document, config=processing_config, now=now)
    persisted_items = persist_item_avatars(
        items,
        output_path=output_path,
        avatar_dir=avatar_dir,
        timeout_seconds=max(1.0, aggregation_config.timeout_seconds),
        workers=1,
    )
    return document, persisted_items


def has_reached_total_limit(items: list[ProcessedItem], config: ProcessingConfig) -> bool:
    now = config.now or datetime.now(UTC)
    return should_stop_after_limit(items, config, now)
