from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
import hashlib
from html.parser import HTMLParser
import json
import logging
from pathlib import Path
import re
from typing import Callable, Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from .models import ProcessedItem, ProcessedOutput
from .url_utils import normalize_http_url

DEFAULT_AVATAR_DIR_NAME = "favicons"
DEFAULT_AVATAR_TIMEOUT_SECONDS = 10.0
DEFAULT_AVATAR_WORKERS = 8
DEFAULT_USER_AGENT = "FeedsAggregator/0.1"

logger = logging.getLogger(__name__)


class AvatarLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.avatar_url: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.avatar_url is not None or tag.lower() != "link":
            return

        attributes = {key.lower(): (value or "") for key, value in attrs}
        rel_tokens = {token.strip().lower() for token in attributes.get("rel", "").split()}
        if not rel_tokens.intersection({"icon", "shortcut", "apple-touch-icon", "apple-touch-icon-precomposed", "mask-icon"}):
            return

        href = attributes.get("href", "").strip()
        if href:
            self.avatar_url = href


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
) -> ProcessedOutput:
    new_items = persist_item_avatars(
        output.items,
        output_path=output_path,
        avatar_dir=avatar_dir,
        timeout_seconds=timeout_seconds,
        workers=workers,
    )
    return ProcessedOutput(items=new_items, updated=output.updated)


def persist_item_avatars(
    items: list[ProcessedItem],
    *,
    output_path: str | Path,
    avatar_dir: str | Path | None = None,
    timeout_seconds: float = DEFAULT_AVATAR_TIMEOUT_SECONDS,
    workers: int = DEFAULT_AVATAR_WORKERS,
) -> list[ProcessedItem]:
    output_file = Path(output_path)
    avatar_root = Path(avatar_dir) if avatar_dir else output_file.parent / DEFAULT_AVATAR_DIR_NAME
    discovery_requests: dict[str, str] = {}
    for item in items:
        if (item.avatar or "").strip():
            continue
        discovery_key = build_discovery_key(item)
        discovery_requests.setdefault(discovery_key, item.link)

    discovery_targets = list(discovery_requests)
    discovery_cache = run_in_parallel(
        discovery_targets,
        lambda key: discover_avatar_url(discovery_requests[key], timeout_seconds=timeout_seconds),
        workers=workers,
    )
    avatar_targets = unique_values(
        (
            ((item.avatar or "").strip() or discovery_cache.get(build_discovery_key(item)) or ""),
            item.feed_domain,
        )
        for item in items
    )
    avatar_targets = [target for target in avatar_targets if target[0]]
    avatar_cache = run_in_parallel(
        avatar_targets,
        lambda target: download_avatar(
            target[0],
            feed_domain=target[1],
            avatar_root=avatar_root,
            timeout_seconds=timeout_seconds,
        ),
        workers=workers,
    )

    new_items = []
    for item in items:
        source_avatar_url = (item.avatar or "").strip() or discovery_cache.get(build_discovery_key(item)) or ""
        if not source_avatar_url:
            new_items.append(item)
            continue

        filename = avatar_cache[(source_avatar_url, item.feed_domain)]
        local_avatar = filename if filename is not None else source_avatar_url
        new_items.append(replace(item, avatar=local_avatar))

    return new_items


def build_discovery_key(item) -> str:
    return item.source_key or item.link


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


def discover_avatar_url(page_url: str, *, timeout_seconds: float) -> str | None:
    parsed = urlparse(page_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    request = Request(
        page_url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = getattr(response, "status", 200)
            if status_code < 200 or status_code >= 300:
                logger.warning("Avatar discovery returned HTTP %s for %s", status_code, page_url)
                return None
            content_type = response.headers.get_content_type()
            if content_type not in {"text/html", "application/xhtml+xml"}:
                return None
            charset = response.headers.get_content_charset() or "utf-8"
            payload = response.read()
    except Exception as exc:
        logger.warning("Avatar discovery failed for %s: %s", page_url, exc)
        return None

    try:
        html_text = payload.decode(charset, errors="replace")
    except LookupError:
        html_text = payload.decode("utf-8", errors="replace")

    parser = AvatarLinkParser()
    parser.feed(html_text)
    if parser.avatar_url is None:
        return None

    resolved = urljoin(page_url, parser.avatar_url)
    normalized = normalize_avatar_url(resolved)
    if normalized is None:
        logger.warning("Avatar discovery found invalid icon URL on %s: %s", page_url, parser.avatar_url)
    return normalized


def download_avatar(
    avatar_url: str,
    *,
    feed_domain: str | None,
    avatar_root: Path,
    timeout_seconds: float,
) -> str | None:
    parsed = urlparse(avatar_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    url_extension = resolve_url_extension(parsed.path)
    if url_extension is not None:
        filename = build_avatar_filename(feed_domain or parsed.hostname or parsed.netloc, avatar_url, url_extension)
        avatar_path = avatar_root / filename
        if avatar_path.exists():
            return filename

    request = Request(avatar_url, headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "image/*,*/*;q=0.1"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = getattr(response, "status", 200)
            if status_code < 200 or status_code >= 300:
                logger.warning("Avatar download returned HTTP %s for %s", status_code, avatar_url)
                return None
            payload = response.read()
            content_type = response.headers.get_content_type()
    except Exception as exc:
        logger.warning("Avatar download failed for %s: %s", avatar_url, exc)
        return None

    if not payload:
        logger.warning("Avatar download returned empty payload for %s", avatar_url)
        return None

    extension = resolve_avatar_extension(parsed.path, content_type=content_type)
    filename = build_avatar_filename(feed_domain or parsed.hostname or parsed.netloc, avatar_url, extension)
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


def build_avatar_filename(domain: str, avatar_url: str, extension: str) -> str:
    normalized_domain = domain.strip().lower().split("?", 1)[0]
    safe_domain = re.sub(r"[^a-zA-Z0-9-]", "_", normalized_domain) or "unknown"
    url_hash = hashlib.sha256(avatar_url.encode("utf-8")).hexdigest()[:16]
    return f"{safe_domain}_{url_hash}{extension}"
