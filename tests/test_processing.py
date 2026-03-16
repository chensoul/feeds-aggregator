from __future__ import annotations

from datetime import UTC, datetime
import unittest
from unittest.mock import patch

from feeds_aggregator.models import AggregationResult, FeedSource, RawFeedDocument, RawFeedEntry
from feeds_aggregator.processing import ProcessingConfig, process_aggregation_result


class ProcessingTests(unittest.TestCase):
    def test_normalizes_and_sorts_items(self):
        source = FeedSource(source_url="https://example.com/feed.xml", category="tech", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="Example Feed",
                    entries=[
                        RawFeedEntry(
                            title="Older",
                            link="https://example.com/older",
                            published="2026-03-12T10:00:00Z",
                        ),
                        RawFeedEntry(
                            title="Newer",
                            link="https://example.com/newer",
                            published="2026-03-13T10:00:00Z",
                        ),
                    ],
                )
            ]
        )

        output = process_aggregation_result(result)

        self.assertEqual(2, len(output.items))
        self.assertEqual("Newer", output.items[0].title)
        self.assertEqual("Older", output.items[1].title)
        self.assertEqual("Example", output.items[0].name)
        self.assertEqual("tech", output.items[0].category)

    def test_deduplicates_by_link(self):
        source = FeedSource(source_url="https://example.com/feed.xml", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="Example Feed",
                    entries=[
                        RawFeedEntry(
                            title="Older",
                            link="https://example.com/post",
                            published="2026-03-12T10:00:00Z",
                        ),
                        RawFeedEntry(
                            title="Newer",
                            link="https://example.com/post",
                            published="2026-03-13T10:00:00Z",
                        ),
                    ],
                )
            ]
        )

        output = process_aggregation_result(result)

        self.assertEqual(1, len(output.items))
        self.assertEqual("Newer", output.items[0].title)

    def test_applies_recent_days_and_total_limit(self):
        source = FeedSource(source_url="https://example.com/feed.xml", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="Example Feed",
                    entries=[
                        RawFeedEntry(
                            title="Keep A",
                            link="https://example.com/a",
                            published="2026-03-13T10:00:00Z",
                        ),
                        RawFeedEntry(
                            title="Keep B",
                            link="https://example.com/b",
                            published="2026-03-12T10:00:00Z",
                        ),
                        RawFeedEntry(
                            title="Drop Old",
                            link="https://example.com/c",
                            published="2026-03-01T10:00:00Z",
                        ),
                    ],
                )
            ]
        )

        output = process_aggregation_result(
            result,
            ProcessingConfig(
                max_total_items=1,
                max_days=2,
                now=datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC),
            ),
        )

        self.assertEqual(1, len(output.items))
        self.assertEqual("Keep A", output.items[0].title)

    def test_stops_processing_additional_documents_when_total_limit_reached(self):
        source = FeedSource(source_url="https://example.com/feed.xml", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="First Feed",
                    entries=[RawFeedEntry(title="First", link="https://example.com/first", published="2026-03-13T10:00:00Z")],
                ),
                RawFeedDocument(
                    source=source,
                    title="Second Feed",
                    entries=[RawFeedEntry(title="Second", link="https://example.com/second", published="2026-03-13T09:00:00Z")],
                ),
            ]
        )

        with patch("feeds_aggregator.processing.normalize_document") as mocked_normalize:
            mocked_normalize.side_effect = [
                [
                    type("Item", (), {
                        "title": "First",
                        "link": "https://example.com/first",
                        "published": "2026-03-13 10:00:00",
                        "name": "Example",
                        "category": None,
                        "avatar": None,
                        "feed_domain": "example.com",
                    })()
                ],
                [
                    type("Item", (), {
                        "title": "Second",
                        "link": "https://example.com/second",
                        "published": "2026-03-13 09:00:00",
                        "name": "Example",
                        "category": None,
                        "avatar": None,
                        "feed_domain": "example.com",
                    })()
                ],
            ]

            output = process_aggregation_result(
                result,
                ProcessingConfig(
                    max_total_items=1,
                    max_days=180,
                    now=datetime(2026, 3, 13, 12, 0, 0, tzinfo=UTC),
                ),
            )

        self.assertEqual(1, mocked_normalize.call_count)
        self.assertEqual(1, len(output.items))

    def test_uses_updated_time_when_published_missing(self):
        source = FeedSource(source_url="https://example.com/feed.xml", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="Example Feed",
                    entries=[
                        RawFeedEntry(
                            title="Updated Only",
                            link="https://example.com/updated",
                            updated="2026-03-13T10:00:00Z",
                        )
                    ],
                )
            ]
        )

        output = process_aggregation_result(result)

        self.assertEqual(1, len(output.items))
        self.assertEqual("2026-03-13 10:00:00", output.items[0].published)

    def test_applies_timezone_to_output(self):
        source = FeedSource(source_url="https://example.com/feed.xml", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="Example Feed",
                    entries=[
                        RawFeedEntry(
                            title="Timezone Post",
                            link="https://example.com/tz",
                            published="2026-03-13T10:00:00Z",
                        )
                    ],
                )
            ]
        )

        output = process_aggregation_result(result, ProcessingConfig(timezone_name="Asia/Shanghai"))

        self.assertEqual("2026-03-13 18:00:00", output.items[0].published)

    def test_uses_feed_avatar_when_available(self):
        source = FeedSource(source_url="https://example.com/feed.xml", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="Example Feed",
                    avatar="https://cdn.example.com/icon.png",
                    entries=[
                        RawFeedEntry(
                            title="Post",
                            link="https://example.com/post",
                            published="2026-03-13T10:00:00Z",
                        )
                    ],
                )
            ]
        )

        output = process_aggregation_result(result)

        self.assertEqual("https://cdn.example.com/icon.png", output.items[0].avatar)

    def test_preserves_feed_homepage_for_avatar_discovery(self):
        source = FeedSource(source_url="https://feeds.feedburner.com/example", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="Example Feed",
                    homepage_url="https://example.com/",
                    entries=[
                        RawFeedEntry(
                            title="Post",
                            link="https://example.com/post",
                            published="2026-03-13T10:00:00Z",
                        )
                    ],
                )
            ]
        )

        output = process_aggregation_result(result)

        self.assertEqual("https://example.com/", output.items[0].source_homepage)

    def test_leaves_avatar_empty_when_feed_avatar_missing(self):
        source = FeedSource(source_url="https://example.com/feed.xml", source_name="Example")
        result = AggregationResult(
            successes=[
                RawFeedDocument(
                    source=source,
                    title="Example Feed",
                    entries=[
                        RawFeedEntry(
                            title="Post",
                            link="https://example.com/post",
                            published="2026-03-13T10:00:00Z",
                        )
                    ],
                )
            ]
        )

        output = process_aggregation_result(result)

        self.assertIsNone(output.items[0].avatar)


if __name__ == "__main__":
    unittest.main()
