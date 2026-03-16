from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
import hashlib
from html.parser import HTMLParser
import json
import logging
from pathlib import Path
import re
from time import sleep
from typing import Callable, Iterable
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .models import ProcessedItem, ProcessedOutput
from .url_utils import normalize_http_url

DEFAULT_AVATAR_DIR_NAME = "avatars"
DEFAULT_AVATAR_TIMEOUT_SECONDS = 10.0
DEFAULT_AVATAR_WORKERS = 8
DEFAULT_AVATAR_DELAY_MS = 200
DEFAULT_AVATAR_DOWNLOAD_ATTEMPTS = 2
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)

logger = logging.getLogger(__name__)


class AvatarLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._candidates_by_priority: dict[int, list[str]] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.lower()
        attributes = {key.lower(): (value or "") for key, value in attrs}
        if normalized_tag == "meta":
            meta_name = attributes.get("name", "").strip().lower()
            meta_property = attributes.get("property", "").strip().lower()
            if meta_name in {"twitter:image", "twitter:image:src"} or meta_property in {"og:image", "og:image:url", "og:image:secure_url"}:
                content = attributes.get("content", "").strip()
                if content:
                    self._add_candidate(content, priority=0)
            return

        if normalized_tag != "link":
            return

        rel_tokens = {token.strip().lower() for token in attributes.get("rel", "").split()}
        priority = resolve_link_avatar_priority(rel_tokens)
        if priority < 0:
            return

        href = attributes.get("href", "").strip()
        if href:
            self._add_candidate(href, priority=priority)

    @property
    def avatar_urls(self) -> list[str]:
        candidates: list[str] = []
        seen: set[str] = set()
        for priority in sorted(self._candidates_by_priority, reverse=True):
            for value in self._candidates_by_priority[priority]:
                if value in seen:
                    continue
                seen.add(value)
                candidates.append(value)
        return candidates

    def _add_candidate(self, value: str, *, priority: int) -> None:
        self._candidates_by_priority.setdefault(priority, []).append(value)


def resolve_link_avatar_priority(rel_tokens: set[str]) -> int:
    if rel_tokens.intersection({"icon", "shortcut"}):
        return 3
    if "image_src" in rel_tokens:
        return 2
    if rel_tokens.intersection({"apple-touch-icon", "apple-touch-icon-precomposed", "mask-icon"}):
        return 1
    return -1


def write_output_file(output: ProcessedOutput, output_path: str | Path) -> Path:
    path = Path(output_path)
    if path.exists() and path.is_dir():
        raise OSError(f"Output path is a directory: {path}")

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = serialize_output(output)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logger.info("Wrote output file to %s with %d items", path, len(output.items))
    return path


def serialize_output(output: ProcessedOutput) -> dict[str, object]:
    formatted = apply_output_formatting(output)
    return {
        "items": [
            {
                "title": item.title,
                "link": item.link,
                "published": item.published,
                "name": item.name,
                "category": item.category,
                "avatar": item.avatar,
            }
            for item in formatted.items
        ],
        "updated": formatted.updated,
    }


def persist_avatars(
    output: ProcessedOutput,
    *,
    output_path: str | Path,
    avatar_dir: str | Path | None = None,
    timeout_seconds: float = DEFAULT_AVATAR_TIMEOUT_SECONDS,
    workers: int = DEFAULT_AVATAR_WORKERS,
    delay_ms: int = DEFAULT_AVATAR_DELAY_MS,
) -> ProcessedOutput:
    new_items = persist_item_avatars(
        output.items,
        output_path=output_path,
        avatar_dir=avatar_dir,
        timeout_seconds=timeout_seconds,
        workers=workers,
        delay_ms=delay_ms,
    )
    return ProcessedOutput(items=new_items, updated=output.updated)


def persist_item_avatars(
    items: list[ProcessedItem],
    *,
    output_path: str | Path,
    avatar_dir: str | Path | None = None,
    timeout_seconds: float = DEFAULT_AVATAR_TIMEOUT_SECONDS,
    workers: int = DEFAULT_AVATAR_WORKERS,
    delay_ms: int = DEFAULT_AVATAR_DELAY_MS,
) -> list[ProcessedItem]:
    output_file = Path(output_path)
    avatar_root = Path(avatar_dir) if avatar_dir else output_file.parent / DEFAULT_AVATAR_DIR_NAME
    discovery_requests: dict[str, str] = {}
    for item in items:
        if (item.avatar or "").strip():
            continue
        discovery_key = build_discovery_key(item)
        discovery_requests.setdefault(discovery_key, build_avatar_discovery_url(item))

    discovery_targets = list(discovery_requests)
    discovery_cache = run_in_parallel(
        discovery_targets,
        lambda key: discover_avatar_urls(discovery_requests[key], timeout_seconds=timeout_seconds, delay_ms=delay_ms),
        workers=workers,
    )
    avatar_targets = unique_values(
        (
            build_avatar_candidate_list(item, discovery_cache.get(build_discovery_key(item)) or []),
            item.feed_domain,
            build_discovery_key(item),
        )
        for item in items
    )
    avatar_targets = [target for target in avatar_targets if target[0]]
    avatar_cache = run_in_parallel(
        avatar_targets,
        lambda target: download_avatar(
            target[0],
            feed_domain=target[1],
            source_identity=target[2],
            avatar_root=avatar_root,
            timeout_seconds=timeout_seconds,
            delay_ms=delay_ms,
        ),
        workers=workers,
    )

    new_items = []
    for item in items:
        source_avatar_urls = build_avatar_candidate_list(item, discovery_cache.get(build_discovery_key(item)) or [])
        if not source_avatar_urls:
            new_items.append(item)
            continue

        filename = avatar_cache[(source_avatar_urls, item.feed_domain, build_discovery_key(item))]
        fallback_avatar = source_avatar_urls[0]
        local_avatar = filename if filename is not None else fallback_avatar
        new_items.append(replace(item, avatar=local_avatar))

    return new_items


def build_discovery_key(item) -> str:
    return item.source_key or item.link


def build_avatar_candidate_list(item: ProcessedItem, discovered_urls: list[str]) -> tuple[str, ...]:
    candidates: list[str] = []
    seen: set[str] = set()
    explicit_avatar = normalize_avatar_url(item.avatar)
    if explicit_avatar is not None:
        candidates.append(explicit_avatar)
        seen.add(explicit_avatar)
    for url in discovered_urls:
        if url in seen:
            continue
        seen.add(url)
        candidates.append(url)
    return tuple(candidates)


def build_avatar_discovery_url(item: ProcessedItem) -> str:
    if item.source_homepage:
        return item.source_homepage
    youtube_channel_url = build_youtube_channel_url(item.source_key)
    if youtube_channel_url is not None:
        return youtube_channel_url
    explicit_avatar = normalize_avatar_url(item.avatar)
    if explicit_avatar is not None:
        return explicit_avatar
    return item.link


def build_youtube_channel_url(source_key: str | None) -> str | None:
    if not source_key:
        return None

    parsed = urlparse(source_key)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"youtube.com", "www.youtube.com"}:
        return None
    if parsed.path.rstrip("/") != "/feeds/videos.xml":
        return None

    channel_id = parse_qs(parsed.query).get("channel_id", [""])[0].strip()
    if not channel_id:
        return None
    return f"{parsed.scheme}://www.youtube.com/channel/{channel_id}"


def run_in_parallel(values: list[T], worker: Callable[[T], R], *, workers: int) -> dict[T, R]:
    if not values:
        return {}
    # workers=1: 与调用方同线程顺序执行，保证「抓取→标准化→avatar」在同一 worker 内闭环
    if workers <= 1:
        return {v: worker(v) for v in values}
    worker_count = min(workers, len(values))
    results: dict[T, R] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_value = {executor.submit(worker, value): value for value in values}
        for future in as_completed(future_to_value):
            value = future_to_value[future]
            results[value] = future.result()
    return results


def unique_values(values: Iterable[T]) -> list[T]:
    unique: list[T] = []
    seen: set[T] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def build_browser_page_request(url: str) -> Request:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else url
    return Request(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Referer": origin,
        },
    )


def build_browser_asset_request(url: str, *, referer: str | None = None) -> Request:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else url
    return Request(
        url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "image",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "cross-site",
            "Referer": referer or origin,
        },
    )


def build_favicon_fallback_url(page_url: str) -> str | None:
    parsed = urlparse(page_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/favicon.ico"


def probe_favicon_url(page_url: str, *, timeout_seconds: float, delay_ms: int) -> str | None:
    favicon_url = build_favicon_fallback_url(page_url)
    if favicon_url is None:
        return None

    request = build_browser_asset_request(favicon_url, referer=page_url)
    maybe_sleep(delay_ms)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = getattr(response, "status", 200)
            if status_code < 200 or status_code >= 300:
                return None
            content_type = response.headers.get_content_type()
            if not content_type.startswith("image/"):
                return None
            return favicon_url
    except Exception:
        return None


def discover_avatar_urls(page_url: str, *, timeout_seconds: float, delay_ms: int) -> list[str]:
    parsed = urlparse(page_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return []

    request = build_browser_page_request(page_url)
    maybe_sleep(delay_ms)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = getattr(response, "status", 200)
            if status_code < 200 or status_code >= 300:
                logger.warning("Avatar discovery returned HTTP %s for %s", status_code, page_url)
                return fallback_avatar_urls(page_url, timeout_seconds=timeout_seconds, delay_ms=delay_ms)
            content_type = response.headers.get_content_type()
            if content_type not in {"text/html", "application/xhtml+xml"}:
                return fallback_avatar_urls(page_url, timeout_seconds=timeout_seconds, delay_ms=delay_ms)
            charset = response.headers.get_content_charset() or "utf-8"
            payload = response.read()
    except Exception as exc:
        logger.warning("Avatar discovery failed for %s: %s", page_url, exc)
        return fallback_avatar_urls(page_url, timeout_seconds=timeout_seconds, delay_ms=delay_ms)

    try:
        html_text = payload.decode(charset, errors="replace")
    except LookupError:
        html_text = payload.decode("utf-8", errors="replace")

    parser = AvatarLinkParser()
    parser.feed(html_text)
    candidates: list[str] = []
    for raw_url in parser.avatar_urls:
        resolved = urljoin(page_url, raw_url)
        normalized = normalize_avatar_url(resolved)
        if normalized is None:
            logger.warning("Avatar discovery found invalid icon URL on %s: %s", page_url, raw_url)
            continue
        candidates.append(normalized)
    if not candidates:
        return fallback_avatar_urls(page_url, timeout_seconds=timeout_seconds, delay_ms=delay_ms)
    return prioritize_avatar_candidates(page_url, candidates)


def fallback_avatar_urls(page_url: str, *, timeout_seconds: float, delay_ms: int) -> list[str]:
    favicon_url = probe_favicon_url(page_url, timeout_seconds=timeout_seconds, delay_ms=delay_ms)
    if favicon_url is None:
        return []
    return [favicon_url]


def prioritize_avatar_candidates(page_url: str, candidates: list[str]) -> list[str]:
    page_parsed = urlparse(page_url)
    if page_parsed.hostname not in {"youtube.com", "www.youtube.com"} or not page_parsed.path.startswith("/channel/"):
        return candidates

    def sort_key(candidate: str) -> tuple[int, int]:
        candidate_host = (urlparse(candidate).hostname or "").lower()
        is_channel_avatar = candidate_host.endswith("googleusercontent.com")
        is_site_icon = candidate_host in {"youtube.com", "www.youtube.com"}
        if is_channel_avatar:
            return (0, 0)
        if is_site_icon:
            return (2, 0)
        return (1, 0)

    return sorted(candidates, key=sort_key)


def download_avatar(
    avatar_urls: tuple[str, ...],
    *,
    feed_domain: str | None,
    source_identity: str,
    avatar_root: Path,
    timeout_seconds: float,
    delay_ms: int,
) -> str | None:
    for avatar_url in avatar_urls:
        parsed = urlparse(avatar_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue

        url_extension = resolve_url_extension(parsed.path)
        if url_extension is not None:
            filename = build_avatar_filename(feed_domain or parsed.hostname or parsed.netloc, source_identity, url_extension)
            avatar_path = avatar_root / filename
            if avatar_path.exists():
                return filename

        payload: bytes | None = None
        content_type = ""
        for attempt in range(1, DEFAULT_AVATAR_DOWNLOAD_ATTEMPTS + 1):
            request = build_browser_asset_request(avatar_url)
            maybe_sleep(delay_ms)
            try:
                with urlopen(request, timeout=timeout_seconds) as response:
                    status_code = getattr(response, "status", 200)
                    if status_code < 200 or status_code >= 300:
                        logger.warning("Avatar download returned HTTP %s for %s", status_code, avatar_url)
                        if should_retry_avatar_status(status_code) and attempt < DEFAULT_AVATAR_DOWNLOAD_ATTEMPTS:
                            continue
                        payload = None
                        break
                    payload = response.read()
                    content_type = response.headers.get_content_type()
                    break
            except Exception as exc:
                logger.warning("Avatar download failed for %s: %s", avatar_url, exc)
                if should_retry_avatar_exception(exc) and attempt < DEFAULT_AVATAR_DOWNLOAD_ATTEMPTS:
                    continue
                payload = None
                break

        if payload is None:
            continue

        if not payload:
            logger.warning("Avatar download returned empty payload for %s", avatar_url)
            continue

        extension = resolve_avatar_extension(parsed.path, content_type=content_type)
        filename = build_avatar_filename(feed_domain or parsed.hostname or parsed.netloc, source_identity, extension)
        try:
            avatar_root.mkdir(parents=True, exist_ok=True)
            avatar_path = avatar_root / filename
            if avatar_path.exists():
                return filename
            avatar_path.write_bytes(payload)
            logger.info("Saved avatar for %s to %s", avatar_url, avatar_path)
            return filename
        except OSError:
            logger.exception("Failed to persist avatar for %s", avatar_url)
            return None
    return None


def should_retry_avatar_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def should_retry_avatar_exception(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, HTTPError):
        return should_retry_avatar_status(exc.code)
    message = str(exc).lower()
    return "timed out" in message or "timeout" in message


def maybe_sleep(delay_ms: int) -> None:
    if delay_ms <= 0:
        return
    sleep(delay_ms / 1000.0)


def apply_output_formatting(output: ProcessedOutput) -> ProcessedOutput:
    new_items = [replace(item, name=normalize_source_name(item.name)) for item in output.items]
    return ProcessedOutput(items=new_items, updated=output.updated)


def normalize_source_name(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return "@"
    if candidate.startswith("@"):
        return candidate
    return f"@{candidate}"


def resolve_url_extension(path: str) -> str | None:
    suffix = Path(path).suffix.lower()
    if re.fullmatch(r"\.[a-z0-9]{1,10}", suffix or ""):
        return suffix
    return None


def normalize_avatar_url(value: str | None) -> str | None:
    return normalize_http_url(value)


def resolve_avatar_extension(path: str, *, content_type: str) -> str:
    suffix = resolve_url_extension(path)
    if suffix is not None:
        return suffix

    if content_type == "image/png":
        return ".png"
    if content_type == "image/jpeg":
        return ".jpg"
    if content_type == "image/webp":
        return ".webp"
    if content_type == "image/gif":
        return ".gif"
    if content_type == "image/svg+xml":
        return ".svg"
    if content_type in {"image/x-icon", "image/vnd.microsoft.icon"}:
        return ".ico"
    return ".img"


def build_avatar_filename(domain: str, source_identity: str, extension: str) -> str:
    normalized_domain = domain.strip().lower().split("?", 1)[0]
    safe_domain = re.sub(r"[^a-zA-Z0-9-]", "_", normalized_domain) or "unknown"
    url_hash = hashlib.sha256(source_identity.encode("utf-8")).hexdigest()[:16]
    return f"{safe_domain}_{url_hash}{extension}"
