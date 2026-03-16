from __future__ import annotations

import xml.etree.ElementTree as ET

from .errors import AggregationError
from .models import FeedSource, RawFeedDocument, RawFeedEntry
from .url_utils import normalize_http_url

ATOM_NS = "{http://www.w3.org/2005/Atom}"


def parse_feed_xml(source: FeedSource, xml_text: str) -> RawFeedDocument:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
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

    return RawFeedDocument(source=source, title=title, entries=entries, avatar=avatar)


def parse_atom(source: FeedSource, root: ET.Element) -> RawFeedDocument:
    title = find_child_text(root, f"{ATOM_NS}title") or source.source_name
    avatar = find_atom_avatar(root)
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

    return RawFeedDocument(source=source, title=title, entries=entries, avatar=avatar)


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
