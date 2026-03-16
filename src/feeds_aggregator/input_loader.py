from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

from .errors import InputValidationError
from .models import FeedSource, InputLoadResult


@dataclass(slots=True, frozen=True)
class SourceLine:
    line_number: int
    raw_value: str


def load_sources(input_path: str | Path) -> InputLoadResult:
    path = Path(input_path)
    if not path.exists():
        raise InputValidationError(f"Input file does not exist: {path}")
    if not path.is_file():
        raise InputValidationError(f"Input path is not a file: {path}")

    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise InputValidationError(f"Input file is not valid UTF-8 text: {path}") from exc

    if not content.strip():
        raise InputValidationError(f"Input file is empty: {path}")

    format_name = detect_input_format(path, content)
    if format_name == "txt":
        sources = parse_text_sources(content)
    elif format_name == "opml":
        sources = parse_opml_sources(content)
    else:
        raise InputValidationError(f"Unsupported input format: {path.suffix or '<none>'}")

    if not sources:
        raise InputValidationError(f"No valid feed sources found in input file: {path}")

    return InputLoadResult(format_name=format_name, sources=sources)


def detect_input_format(path: Path, content: str) -> str:
    suffix = path.suffix.lower()
    looks_like_xml = content.lstrip().startswith("<?xml") or "<opml" in content.lower()

    if suffix == ".txt":
        if looks_like_xml:
            raise InputValidationError("Input file extension is .txt but content looks like OPML/XML")
        return "txt"

    if suffix == ".opml":
        if not looks_like_xml:
            raise InputValidationError("Input file extension is .opml but content does not look like OPML/XML")
        return "opml"

    if looks_like_xml:
        return "opml"
    return "txt"


def parse_text_sources(content: str) -> list[FeedSource]:
    sources: list[FeedSource] = []

    for source_line in iter_source_lines(content):
        raw = source_line.raw_value
        category: str | None = None
        url = raw

        if "," in raw:
            category_part, url_part = raw.split(",", 1)
            category = normalize_optional_value(category_part)
            url = url_part.strip()

        validate_url(url, context=f"line {source_line.line_number}")
        sources.append(FeedSource(source_url=url, category=category))

    return sources


def iter_source_lines(content: str) -> list[SourceLine]:
    source_lines: list[SourceLine] = []
    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        value = raw_line.strip()
        if not value or value.startswith("#"):
            continue
        source_lines.append(SourceLine(line_number=line_number, raw_value=value))
    return source_lines


def parse_opml_sources(content: str) -> list[FeedSource]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise InputValidationError("OPML content is not valid XML") from exc

    if root.tag.lower() != "opml":
        raise InputValidationError("OPML input must use an <opml> root element")

    body = root.find("body")
    if body is None:
        raise InputValidationError("OPML input must contain a <body> element")

    sources: list[FeedSource] = []
    walk_opml_nodes(body, [], sources)
    return sources


def walk_opml_nodes(node: ET.Element, category_stack: list[str], sources: list[FeedSource]) -> None:
    for child in list(node):
        child_text = normalize_optional_value(child.attrib.get("text") or child.attrib.get("title"))
        explicit_category = normalize_opml_category(child.attrib.get("category"))
        xml_url = normalize_optional_value(child.attrib.get("xmlUrl"))

        next_stack = category_stack
        if xml_url is None:
            inherited_category = explicit_category or child_text
            if inherited_category:
                next_stack = [*category_stack, inherited_category]

        if xml_url:
            validate_url(xml_url, context=f"OPML outline '{child_text or xml_url}'")
            category = explicit_category or (next_stack[-1] if next_stack else None)
            sources.append(
                FeedSource(
                    source_url=xml_url,
                    category=category,
                    source_name=child_text,
                )
            )

        if list(child):
            walk_opml_nodes(child, next_stack, sources)


def normalize_optional_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def normalize_opml_category(value: str | None) -> str | None:
    normalized = normalize_optional_value(value)
    if normalized is None:
        return None
    if "," not in normalized:
        return normalized
    first = normalized.split(",", 1)[0].strip()
    return first or None


def validate_url(url: str, *, context: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise InputValidationError(f"Invalid feed source URL in {context}: {url}")
