from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import AggregationResult, ProcessedItem, ProcessedOutput, RawFeedDocument, RawFeedEntry
from .url_utils import normalize_http_url

DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_TIMEZONE = "UTC"
DEFAULT_MAX_ITEMS_PER_SOURCE = 10
DEFAULT_MAX_TOTAL_ITEMS = 0
DEFAULT_MAX_DAYS = 180


@dataclass(slots=True, frozen=True)
class ProcessingConfig:
    max_items_per_source: int = DEFAULT_MAX_ITEMS_PER_SOURCE
    max_total_items: int = DEFAULT_MAX_TOTAL_ITEMS
    max_days: int = DEFAULT_MAX_DAYS
    deduplicate: bool = True
    now: datetime | None = None
    timezone_name: str = DEFAULT_TIMEZONE


def process_aggregation_result(
    aggregation: AggregationResult,
    config: ProcessingConfig | None = None,
) -> ProcessedOutput:
    active_config = config or ProcessingConfig()
    now = active_config.now or datetime.now(UTC)

    processed_items: list[ProcessedItem] = []
    for document in aggregation.successes:
        items = process_document(document, config=active_config, now=now)
        processed_items.extend(items)
        if should_stop_after_limit(processed_items, active_config, now):
            break

    return build_processed_output(processed_items, config=active_config, now=now)


def process_document(
    document: RawFeedDocument,
    *,
    config: ProcessingConfig,
    now: datetime,
) -> list[ProcessedItem]:
    items = normalize_document(document, timezone_name=config.timezone_name)
    items = apply_per_source_limit(items, config.max_items_per_source)
    items = apply_recent_days_filter(items, config.max_days, now)
    return items


def build_processed_output(
    items: list[ProcessedItem],
    *,
    config: ProcessingConfig,
    now: datetime,
) -> ProcessedOutput:
    timezone = resolve_timezone(config.timezone_name)
    processed_items = list(items)
    if config.deduplicate:
        processed_items = deduplicate_items(processed_items)
    processed_items = sort_items(processed_items)
    processed_items = apply_total_limit(processed_items, config.max_total_items)

    updated = format_datetime(now, timezone=timezone)
    return ProcessedOutput(items=processed_items, updated=updated)


def normalize_document(document: RawFeedDocument, *, timezone_name: str) -> list[ProcessedItem]:
    name = choose_source_name(document)
    avatar = choose_avatar(document)
    feed_domain = determine_feed_domain(document)
    source_key = document.source.source_url
    timezone = resolve_timezone(timezone_name)
    items: list[ProcessedItem] = []

    for entry in document.entries:
        published_dt = choose_entry_datetime(entry)
        if published_dt is None:
            continue
        if not entry.title.strip() or not entry.link.strip():
            continue
        items.append(
            ProcessedItem(
                title=entry.title.strip(),
                link=entry.link.strip(),
                published=format_datetime(published_dt, timezone=timezone),
                name=name,
                category=document.source.category,
                avatar=avatar,
                feed_domain=feed_domain,
                source_key=source_key,
            )
        )

    return sort_items(items)


def choose_source_name(document: RawFeedDocument) -> str:
    for candidate in [document.source.source_name, document.title, document.source.source_url]:
        if candidate and candidate.strip():
            return candidate.strip()
    return "Unknown Source"


def choose_avatar(document: RawFeedDocument) -> str | None:
    explicit_avatar = normalize_http_url(document.avatar)
    if explicit_avatar:
        return explicit_avatar
    return None


def determine_feed_domain(document: RawFeedDocument) -> str | None:
    parsed = urlparse(document.source.source_url)
    hostname = (parsed.hostname or "").strip().lower()
    return hostname or None


def choose_entry_datetime(entry: RawFeedEntry) -> datetime | None:
    for value in [entry.published, entry.updated]:
        parsed = parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    raw = value.strip()
    if not raw:
        return None

    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return ensure_utc(parsed)
    except ValueError:
        pass

    try:
        parsed = parsedate_to_datetime(raw)
        return ensure_utc(parsed)
    except (TypeError, ValueError, IndexError):
        return None


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def resolve_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone: {timezone_name}") from exc


def format_datetime(value: datetime, *, timezone: ZoneInfo | None = None) -> str:
    tz = timezone or resolve_timezone(DEFAULT_TIMEZONE)
    return ensure_utc(value).astimezone(tz).strftime(DEFAULT_TIME_FORMAT)


def apply_per_source_limit(items: list[ProcessedItem], limit: int) -> list[ProcessedItem]:
    if limit <= 0:
        return items
    return items[:limit]


def apply_recent_days_filter(items: list[ProcessedItem], max_days: int, now: datetime) -> list[ProcessedItem]:
    if max_days <= 0:
        return items
    cutoff = ensure_utc(now).timestamp() - (max_days * 24 * 60 * 60)
    filtered: list[ProcessedItem] = []
    for item in items:
        item_dt = parse_datetime(item.published)
        if item_dt is None:
            continue
        if item_dt.timestamp() >= cutoff:
            filtered.append(item)
    return filtered


def deduplicate_items(items: list[ProcessedItem]) -> list[ProcessedItem]:
    deduped: dict[str, ProcessedItem] = {}
    for item in items:
        existing = deduped.get(item.link)
        if existing is None:
            deduped[item.link] = item
            continue
        if compare_items(item, existing) < 0:
            deduped[item.link] = item
    return list(deduped.values())


def sort_items(items: list[ProcessedItem]) -> list[ProcessedItem]:
    return sorted(items, key=sort_key)


def sort_key(item: ProcessedItem) -> tuple[float, str]:
    item_dt = parse_datetime(item.published)
    timestamp = item_dt.timestamp() if item_dt is not None else 0.0
    return (-timestamp, item.link)


def compare_items(left: ProcessedItem, right: ProcessedItem) -> int:
    left_key = sort_key(left)
    right_key = sort_key(right)
    if left_key < right_key:
        return -1
    if left_key > right_key:
        return 1
    return 0


def apply_total_limit(items: list[ProcessedItem], limit: int) -> list[ProcessedItem]:
    if limit <= 0:
        return items
    return items[:limit]


def should_stop_after_limit(
    items: list[ProcessedItem],
    config: ProcessingConfig,
    now: datetime,
) -> bool:
    if config.max_total_items <= 0:
        return False

    candidate_items = apply_recent_days_filter(items, config.max_days, now)
    if config.deduplicate:
        candidate_items = deduplicate_items(candidate_items)
    return len(candidate_items) >= config.max_total_items
