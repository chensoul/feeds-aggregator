from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urlunparse

from .errors import AggregationError
from .models import FeedSource, RawFeedDocument, RawFeedEntry
from .url_utils import normalize_http_url

ATOM_NS = "{http://www.w3.org/2005/Atom}"
INVALID_XML_CHARACTERS = re.compile(
    "["  # XML 1.0 disallowed control characters
    "\x00-\x08"
    "\x0B-\x0C"
    "\x0E-\x1F"
    "]"
)
UNESCAPED_AMPERSAND = re.compile(r"&(?!#\d+;|#x[0-9A-Fa-f]+;|[A-Za-z_][A-Za-z0-9._-]*;)")


def parse_feed_xml(source: FeedSource, xml_text: str) -> RawFeedDocument:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        sanitized_xml_text = sanitize_xml_text(xml_text)
        if sanitized_xml_text == xml_text:
            raise AggregationError(f"Failed to parse feed XML: {exc}") from exc

        try:
            root = ET.fromstring(sanitized_xml_text)
        except ET.ParseError:
            raise AggregationError(f"Failed to parse feed XML: {exc}") from exc

    tag = normalize_tag(root.tag)
    if tag == "rss":
        return parse_rss(source, root)
    if tag == "feed":
        return parse_atom(source, root)
    raise AggregationError(f"Unsupported feed format: root element <{tag}>")


def parse_rss(source: FeedSource, root: ET.Element) -> RawFeedDocument:
    channel = root.find("channel")
    if channel is None:
        raise AggregationError("RSS feed is missing <channel>")

    title = find_child_text(channel, "title") or source.source_name
    avatar = find_rss_avatar(channel)
    homepage_url = normalize_homepage_url(find_child_text(channel, "link"), source.source_url)
    entries: list[RawFeedEntry] = []
    for item in channel.findall("item"):
        item_title = find_child_text(item, "title")
        item_link = find_child_text(item, "link")
        if not item_title or not item_link:
            continue
        entries.append(
            RawFeedEntry(
                title=item_title,
                link=item_link,
                published=find_child_text(item, "pubDate"),
                updated=find_child_text(item, "lastBuildDate") or find_child_text(item, "updated"),
            )
        )

    return RawFeedDocument(source=source, title=title, entries=entries, avatar=avatar, homepage_url=homepage_url)


def parse_atom(source: FeedSource, root: ET.Element) -> RawFeedDocument:
    title = find_child_text(root, f"{ATOM_NS}title") or source.source_name
    avatar = find_atom_avatar(root)
    homepage_url = normalize_homepage_url(find_atom_link(root), source.source_url)
    entries: list[RawFeedEntry] = []

    for entry in root.findall(f"{ATOM_NS}entry"):
        entry_title = find_child_text(entry, f"{ATOM_NS}title")
        entry_link = find_atom_link(entry)
        if not entry_title or not entry_link:
            continue
        entries.append(
            RawFeedEntry(
                title=entry_title,
                link=entry_link,
                published=find_child_text(entry, f"{ATOM_NS}published"),
                updated=find_child_text(entry, f"{ATOM_NS}updated"),
            )
        )

    return RawFeedDocument(source=source, title=title, entries=entries, avatar=avatar, homepage_url=homepage_url)


def find_atom_link(entry: ET.Element) -> str | None:
    for link in entry.findall(f"{ATOM_NS}link"):
        rel = (link.attrib.get("rel") or "alternate").strip()
        href = (link.attrib.get("href") or "").strip()
        if href and rel == "alternate":
            return href
    for link in entry.findall(f"{ATOM_NS}link"):
        href = (link.attrib.get("href") or "").strip()
        if href:
            return href
    return None


def find_child_text(node: ET.Element, child_name: str) -> str | None:
    child = node.find(child_name)
    if child is None or child.text is None:
        return None
    value = child.text.strip()
    return value or None


def normalize_tag(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def find_rss_avatar(channel: ET.Element) -> str | None:
    image = channel.find("image")
    if image is not None:
        candidate = normalize_optional_url(find_child_text(image, "url"))
        if candidate:
            return candidate

    for child in list(channel):
        if normalize_tag(child.tag).lower() != "image":
            continue
        candidate = normalize_optional_url(
            child.attrib.get("href") or child.attrib.get("url") or child.text
        )
        if candidate:
            return candidate
    return None


def find_atom_avatar(root: ET.Element) -> str | None:
    for tag_name in (f"{ATOM_NS}icon", f"{ATOM_NS}logo"):
        candidate = normalize_optional_url(find_child_text(root, tag_name))
        if candidate:
            return candidate

    for child in list(root):
        child_tag = normalize_tag(child.tag).lower()
        if child_tag not in {"icon", "logo", "image"}:
            continue
        candidate = normalize_optional_url(
            child.attrib.get("href") or child.attrib.get("url") or child.text
        )
        if candidate:
            return candidate
    return None


def normalize_optional_url(value: str | None) -> str | None:
    return normalize_http_url(value)


def normalize_homepage_url(value: str | None, source_url: str) -> str | None:
    candidate = normalize_optional_url(value)
    if candidate is None:
        return None

    source_parsed = urlparse(source_url)
    candidate_parsed = urlparse(candidate)
    if source_parsed.scheme != candidate_parsed.scheme or source_parsed.netloc != candidate_parsed.netloc:
        return candidate

    normalized_path = candidate_parsed.path.rstrip("/").lower()
    if normalized_path in {"/feed", "/rss", "/rss.xml", "/atom", "/atom.xml", "/feed.xml"}:
        return urlunparse((candidate_parsed.scheme, candidate_parsed.netloc, "/", "", "", ""))
    return candidate


def sanitize_xml_text(xml_text: str) -> str:
    sanitized = INVALID_XML_CHARACTERS.sub("", xml_text)
    sanitized = UNESCAPED_AMPERSAND.sub("&amp;", sanitized)
    return sanitized
