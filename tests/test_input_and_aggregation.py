from __future__ import annotations

from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from feeds_aggregator.aggregator import AggregationConfig, aggregate_sources, resolve_worker_count
from feeds_aggregator.input_loader import load_sources
from feeds_aggregator.models import FeedSource

RSS_SAMPLE = """<?xml version='1.0' encoding='UTF-8'?>
<rss version="2.0">
  <channel>
    <title>Sample RSS</title>
    <image>
      <url>https://example.com/rss-avatar.png</url>
    </image>
    <item>
      <title>Post One</title>
      <link>https://example.com/post-1</link>
      <pubDate>Thu, 13 Mar 2026 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

ATOM_SAMPLE = """<?xml version='1.0' encoding='utf-8'?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Sample Atom</title>
  <icon>https://example.com/atom-avatar.png</icon>
  <entry>
    <title>Atom Post</title>
    <link href="https://example.com/atom-post" rel="alternate" />
    <updated>2026-03-13T10:00:00Z</updated>
  </entry>
</feed>
"""


class MockHttpResponse:
    def __init__(self, payload: str, *, content_type: str, status: int = 200):
        self._payload = payload.encode("utf-8")
        self.status = status
        self.headers = Message()
        self.headers["Content-Type"] = content_type

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> MockHttpResponse:
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def build_mock_response(request, timeout=None):
    url = getattr(request, "full_url", request)
    if url.endswith("/rss.xml"):
        return MockHttpResponse(RSS_SAMPLE, content_type="application/rss+xml; charset=utf-8")
    if url.endswith("/atom.xml"):
        return MockHttpResponse(ATOM_SAMPLE, content_type="application/atom+xml; charset=utf-8")
    return MockHttpResponse("missing", content_type="text/plain; charset=utf-8", status=404)


class InputAndAggregationTests(unittest.TestCase):
    def test_resolve_worker_count_clamps_to_source_count(self):
        self.assertEqual(2, resolve_worker_count(8, 2))

    def test_resolve_worker_count_defaults_to_one_for_empty_inputs(self):
        self.assertEqual(1, resolve_worker_count(0, 0))

    def test_load_text_sources_with_category(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sources.txt"
            path.write_text("tech,https://feeds.example.com/rss.xml\n", encoding="utf-8")
            result = load_sources(path)

        self.assertEqual("txt", result.format_name)
        self.assertEqual(1, len(result.sources))
        self.assertEqual("tech", result.sources[0].category)

    def test_load_opml_sources(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sources.opml"
            path.write_text(
                """<?xml version='1.0' encoding='UTF-8'?>
<opml version="2.0"><body><outline text="Blogs"><outline text="Sample" xmlUrl="https://feeds.example.com/rss.xml" /></outline></body></opml>
""",
                encoding="utf-8",
            )
            result = load_sources(path)

        self.assertEqual("opml", result.format_name)
        self.assertEqual(1, len(result.sources))
        self.assertEqual("Blogs", result.sources[0].category)
        self.assertEqual("Sample", result.sources[0].source_name)

    def test_load_opml_sources_with_outline_category_attribute(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sources.opml"
            path.write_text(
                """<?xml version='1.0' encoding='UTF-8'?>
<opml version="2.0"><body><outline text="Blogs"><outline text="Sample" category="tech,dev" xmlUrl="https://feeds.example.com/rss.xml" /></outline></body></opml>
""",
                encoding="utf-8",
            )
            result = load_sources(path)

        self.assertEqual("opml", result.format_name)
        self.assertEqual(1, len(result.sources))
        self.assertEqual("tech", result.sources[0].category)
        self.assertEqual("Sample", result.sources[0].source_name)

    def test_aggregate_sources_handles_partial_failure(self):
        sources = [
            FeedSource(source_url="https://feeds.example.com/rss.xml", category="blog"),
            FeedSource(source_url="https://feeds.example.com/missing.xml", category="blog"),
        ]

        with patch("feeds_aggregator.aggregator.urlopen", side_effect=build_mock_response):
            result = aggregate_sources(sources, AggregationConfig(timeout_seconds=2.0, workers=2))

        self.assertEqual("partial_success", result.outcome)
        self.assertEqual(1, len(result.successes))
        self.assertEqual(1, len(result.failures))
        self.assertEqual(1, result.total_entries)
        self.assertEqual("https://example.com/rss-avatar.png", result.successes[0].avatar)

    def test_aggregate_sources_parses_atom_avatar(self):
        sources = [
            FeedSource(source_url="https://feeds.example.com/atom.xml", category="blog"),
        ]

        with patch("feeds_aggregator.aggregator.urlopen", side_effect=build_mock_response):
            result = aggregate_sources(sources, AggregationConfig(timeout_seconds=2.0, workers=1))

        self.assertEqual("success", result.outcome)
        self.assertEqual(1, len(result.successes))
        self.assertEqual("https://example.com/atom-avatar.png", result.successes[0].avatar)

    def test_aggregate_sources_fails_when_all_sources_fail(self):
        sources = [
            FeedSource(source_url="https://feeds.example.com/missing.xml"),
        ]

        with patch("feeds_aggregator.aggregator.urlopen", side_effect=build_mock_response):
            result = aggregate_sources(sources, AggregationConfig(timeout_seconds=2.0, workers=1))

        self.assertEqual("failure", result.outcome)
        self.assertEqual(0, len(result.successes))
        self.assertEqual(1, len(result.failures))


if __name__ == "__main__":
    unittest.main()
