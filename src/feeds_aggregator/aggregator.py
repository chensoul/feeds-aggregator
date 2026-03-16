from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import logging
import random
from threading import BoundedSemaphore
from time import sleep
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from .errors import AggregationError
from .feed_parser import parse_feed_xml
from .models import AggregationResult, FeedSource, RawFeedDocument, SourceAggregationFailure

DEFAULT_TIMEOUT_SECONDS = 15.0
DEFAULT_WORKERS = 8
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)
DEFAULT_FETCH_ATTEMPTS = 2
YOUTUBE_FETCH_DELAY_MIN_SECONDS = 0.3
YOUTUBE_FETCH_DELAY_MAX_SECONDS = 0.8
YOUTUBE_FETCH_MAX_CONCURRENCY = 1

youtube_fetch_semaphore = BoundedSemaphore(value=YOUTUBE_FETCH_MAX_CONCURRENCY)

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
    request = build_source_request(source.source_url, user_agent=config.user_agent)

    last_error: Exception | None = None
    payload = b""
    charset = "utf-8"
    status_code = 200
    for attempt in range(1, DEFAULT_FETCH_ATTEMPTS + 1):
        try:
            with maybe_throttle_source_fetch(source.source_url):
                with urlopen(request, timeout=config.timeout_seconds) as response:
                    charset = response.headers.get_content_charset() or "utf-8"
                    payload = response.read()
                    status_code = getattr(response, "status", 200)
        except Exception as exc:
            last_error = exc
            if should_retry_fetch_exception(exc) and attempt < DEFAULT_FETCH_ATTEMPTS:
                logger.warning("Retrying source fetch for %s after attempt %d failed: %s", source.source_url, attempt, exc)
                continue
            raise AggregationError(f"Failed to fetch source: {exc}") from exc

        if status_code < 200 or status_code >= 300:
            if should_retry_fetch_status(status_code) and attempt < DEFAULT_FETCH_ATTEMPTS:
                logger.warning("Retrying source fetch for %s after HTTP %s", source.source_url, status_code)
                continue
            raise AggregationError(f"Unexpected HTTP status: {status_code}")
        break

    if last_error is not None and not payload:
        raise AggregationError(f"Failed to fetch source: {last_error}")

    try:
        xml_text = payload.decode(charset, errors="replace")
    except LookupError:
        xml_text = payload.decode("utf-8", errors="replace")

    document = parse_feed_xml(source, xml_text)
    if not document.entries:
        raise AggregationError("Feed returned no usable entries")

    logger.debug("Parsed source %s as %d entries", source.source_url, len(document.entries))
    return document


def should_retry_fetch_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def should_retry_fetch_exception(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, HTTPError):
        return should_retry_fetch_status(exc.code)
    message = str(exc).lower()
    return "timed out" in message or "timeout" in message


def build_source_request(source_url: str, *, user_agent: str) -> Request:
    parsed = urlparse(source_url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else source_url
    return Request(
        source_url,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.5",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": origin,
        },
    )


def is_youtube_feed_url(source_url: str) -> bool:
    parsed = urlparse(source_url)
    if parsed.hostname not in {"www.youtube.com", "youtube.com"}:
        return False
    if parsed.path != "/feeds/videos.xml":
        return False
    channel_ids = parse_qs(parsed.query).get("channel_id", [])
    return bool(channel_ids and channel_ids[0].strip())


class maybe_throttle_source_fetch:
    def __init__(self, source_url: str):
        self.source_url = source_url
        self._use_youtube_guard = is_youtube_feed_url(source_url)

    def __enter__(self):
        if not self._use_youtube_guard:
            return None
        youtube_fetch_semaphore.acquire()
        delay_seconds = random.uniform(YOUTUBE_FETCH_DELAY_MIN_SECONDS, YOUTUBE_FETCH_DELAY_MAX_SECONDS)
        logger.info("Applying YouTube fetch delay of %.3fs for %s", delay_seconds, self.source_url)
        sleep(delay_seconds)
        return None

    def __exit__(self, exc_type, exc, tb):
        if self._use_youtube_guard:
            youtube_fetch_semaphore.release()
        return False
