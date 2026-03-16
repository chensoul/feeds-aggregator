from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(slots=True, frozen=True)
class FeedSource:
    source_url: str
    category: str | None = None
    source_name: str | None = None


@dataclass(slots=True, frozen=True)
class InputLoadResult:
    format_name: str
    sources: list[FeedSource]


@dataclass(slots=True, frozen=True)
class RawFeedEntry:
    title: str
    link: str
    published: str | None = None
    updated: str | None = None


@dataclass(slots=True, frozen=True)
class RawFeedDocument:
    source: FeedSource
    title: str | None
    entries: list[RawFeedEntry]
    avatar: str | None = None
    homepage_url: str | None = None


@dataclass(slots=True, frozen=True)
class SourceAggregationFailure:
    source: FeedSource
    error: str


@dataclass(slots=True, frozen=True)
class AggregationResult:
    successes: list[RawFeedDocument] = field(default_factory=list)
    failures: list[SourceAggregationFailure] = field(default_factory=list)

    @property
    def total_sources(self) -> int:
        return len(self.successes) + len(self.failures)

    @property
    def total_entries(self) -> int:
        return sum(len(document.entries) for document in self.successes)

    @property
    def outcome(self) -> Literal["success", "partial_success", "failure"]:
        if not self.successes:
            return "failure"
        if self.failures:
            return "partial_success"
        return "success"


@dataclass(slots=True, frozen=True)
class ProcessedItem:
    title: str
    link: str
    published: str
    name: str
    category: str | None = None
    avatar: str | None = None
    feed_domain: str | None = None
    source_key: str | None = None
    source_homepage: str | None = None


@dataclass(slots=True, frozen=True)
class ProcessedOutput:
    items: list[ProcessedItem]
    updated: str
