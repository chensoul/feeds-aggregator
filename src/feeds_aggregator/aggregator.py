from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import logging
from urllib.request import Request, urlopen

from .errors import AggregationError
from .feed_parser import parse_feed_xml
from .models import AggregationResult, FeedSource, RawFeedDocument, SourceAggregationFailure

DEFAULT_TIMEOUT_SECONDS = 15.0
DEFAULT_WORKERS = 8
DEFAULT_USER_AGENT = "FeedsAggregator/0.1"

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class AggregationConfig:
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    workers: int = DEFAULT_WORKERS
    user_agent: str = DEFAULT_USER_AGENT


def resolve_worker_count(requested_workers: int, source_count: int) -> int:
    if source_count <= 0:
        return 1
    return max(1, min(requested_workers, source_count))


def aggregate_sources(sources: list[FeedSource], config: AggregationConfig | None = None) -> AggregationResult:
    if not sources:
        logger.info("No sources provided for aggregation")
        return AggregationResult()

    active_config = config or AggregationConfig()
    workers = resolve_worker_count(active_config.workers, len(sources))
    successes: list[RawFeedDocument] = []
    failures: list[SourceAggregationFailure] = []
    logger.info("Starting aggregation for %d sources with %d workers", len(sources), workers)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_source = {
            executor.submit(fetch_and_parse_source, source, active_config): source
            for source in sources
        }
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                document = future.result()
            except AggregationError as exc:
                logger.warning("Source failed: %s (%s)", source.source_url, exc)
                failures.append(SourceAggregationFailure(source=source, error=str(exc)))
                continue
            except Exception as exc:
                logger.exception("Unexpected source failure: %s", source.source_url)
                failures.append(SourceAggregationFailure(source=source, error=f"Unexpected error: {exc}"))
                continue
            logger.info("Source succeeded: %s (%d entries)", source.source_url, len(document.entries))
            successes.append(document)

    logger.info("Aggregation finished: %d successes, %d failures", len(successes), len(failures))
    return AggregationResult(successes=successes, failures=failures)


def fetch_and_parse_source(source: FeedSource, config: AggregationConfig) -> RawFeedDocument:
    logger.debug("Fetching source %s", source.source_url)
    request = Request(
        source.source_url,
        headers={
            "User-Agent": config.user_agent,
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5",
        },
    )

    try:
        with urlopen(request, timeout=config.timeout_seconds) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            payload = response.read()
            status_code = getattr(response, "status", 200)
    except Exception as exc:
        raise AggregationError(f"Failed to fetch source: {exc}") from exc

    if status_code < 200 or status_code >= 300:
        raise AggregationError(f"Unexpected HTTP status: {status_code}")

    try:
        xml_text = payload.decode(charset, errors="replace")
    except LookupError:
        xml_text = payload.decode("utf-8", errors="replace")

    document = parse_feed_xml(source, xml_text)
    if not document.entries:
        raise AggregationError("Feed returned no usable entries")

    logger.debug("Parsed source %s as %d entries", source.source_url, len(document.entries))
    return document
