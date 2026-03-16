from __future__ import annotations

import io
import hashlib
import json
from email.message import Message
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from feeds_aggregator.application import RunAggregationRequest, RunAggregationResult, run_aggregation
from feeds_aggregator.aggregator import AggregationConfig
from feeds_aggregator.cli import FAILURE_EXIT_CODE, INPUT_ERROR_EXIT_CODE, LOG_MESSAGE_FORMAT, LOG_TIME_FORMAT, SUCCESS_EXIT_CODE, build_parser, build_summary_payload, configure_logging, main
from feeds_aggregator.errors import InputValidationError
from feeds_aggregator.failure_log import write_failure_log
from feeds_aggregator.models import AggregationResult, FeedSource, InputLoadResult, ProcessedItem, ProcessedOutput, RawFeedDocument, RawFeedEntry, SourceAggregationFailure
from feeds_aggregator.output_writer import build_avatar_filename, persist_avatars, write_output_file
from feeds_aggregator.processing import ProcessingConfig
from feeds_aggregator.reporting import TaskReport, build_task_report
from feeds_aggregator.runner import process_sources_to_items


class MockHttpResponse:
    def __init__(self, payload: bytes, content_type: str = "image/png", status: int = 200):
        self._payload = payload
        self.status = status
        self.headers = Message()
        self.headers["Content-Type"] = content_type

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> MockHttpResponse:
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class OutputAndReportingTests(unittest.TestCase):
    def test_readme_documents_all_cli_options(self):
        parser = build_parser()
        readme = Path("README.md").read_text(encoding="utf-8")

        for action in parser._actions:
            if not action.option_strings:
                continue
            if action.dest == "help":
                continue
            option = action.option_strings[0]
            self.assertIn(f"`{option}`", readme)

    def test_readme_documents_summary_payload_fields(self):
        readme = Path("README.md").read_text(encoding="utf-8")

        for field_name in [
            '"outcome"',
            '"total_sources"',
            '"successful_sources"',
            '"failed_sources"',
            '"output_items"',
            '"duration_seconds"',
            '"output_path"',
            '"failure_log_path"',
            '"validated_only"',
            '"failed_feed_urls"',
        ]:
            self.assertIn(field_name, readme)

    def test_action_inputs_match_cli_options(self):
        parser = build_parser()
        action_text = Path("action.yml").read_text(encoding="utf-8")
        workflow_text = Path(".github/workflows/feeds-aggregator.yml").read_text(encoding="utf-8")

        expected_inputs = {
            option.lstrip("-"): option
            for action in parser._actions
            for option in action.option_strings[:1]
            if option.startswith("--")
        }

        for input_name, option in expected_inputs.items():
            self.assertRegex(action_text, rf"(?m)^  {re.escape(input_name)}:")
            self.assertRegex(workflow_text, rf"(?m)^      {re.escape(input_name)}:")
            if input_name in {"sources", "output"}:
                self.assertIn(f'--{input_name} "${{{{ inputs.{input_name} }}}}"', action_text)
            elif input_name == "validate-only":
                self.assertIn('if [ "${{ inputs.validate-only }}" = "true" ]; then args+=(--validate-only); fi', action_text)
            else:
                self.assertIn(f"args+=(--{input_name} ", action_text)
            self.assertIn(f"{input_name}: ${{{{ inputs.{input_name} }}}}", workflow_text)

    def test_cli_parser_uses_expected_defaults(self):
        parser = build_parser()

        args = parser.parse_args(["--sources", "data/rss.txt"])

        self.assertEqual(10, args.max_items_per_source)
        self.assertEqual(0, args.max_total_items)
        self.assertEqual(180, args.max_days)
        self.assertEqual("UTC", args.timezone)

    def test_configure_logging_uses_timestamped_format(self):
        with patch("feeds_aggregator.cli.logging.basicConfig") as mocked_basic_config:
            configure_logging()

        mocked_basic_config.assert_called_once_with(
            level=mocked_basic_config.call_args.kwargs["level"],
            format=LOG_MESSAGE_FORMAT,
            datefmt=LOG_TIME_FORMAT,
        )

    def test_cli_parser_rejects_non_positive_workers(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["--sources", "data/rss.txt", "--workers", "0"])

    def test_cli_parser_rejects_non_positive_timeout(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["--sources", "data/rss.txt", "--timeout", "0"])

    def test_cli_parser_rejects_negative_max_days(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["--sources", "data/rss.txt", "--max-days", "-1"])

    def test_cli_parser_rejects_negative_max_items_per_source(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["--sources", "data/rss.txt", "--max-items-per-source", "-1"])

    def test_cli_parser_supports_validate_only(self):
        parser = build_parser()

        args = parser.parse_args(["--sources", "data/rss.txt", "--validate-only"])

        self.assertTrue(args.validate_only)

    def test_cli_parser_supports_failure_log(self):
        parser = build_parser()

        args = parser.parse_args(["--sources", "data/rss.txt", "--failure-log", "data/failures.json"])

        self.assertEqual("data/failures.json", args.failure_log)

    def test_write_output_file_creates_expected_json(self):
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    category="tech",
                    avatar=None,
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nested" / "feeds.json"
            written = write_output_file(output, path)
            payload = json.loads(written.read_text(encoding="utf-8"))

        self.assertEqual(str(path), str(written))
        self.assertEqual("2026-03-13 12:00:00", payload["updated"])
        self.assertEqual(1, len(payload["items"]))
        self.assertEqual("Post", payload["items"][0]["title"])
        self.assertEqual("@Example", payload["items"][0]["name"])

    def test_write_output_file_does_not_duplicate_name_prefix(self):
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="@Example",
                    avatar=None,
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "feeds.json"
            written = write_output_file(output, path)
            payload = json.loads(written.read_text(encoding="utf-8"))

        self.assertEqual("@Example", payload["items"][0]["name"])

    def test_build_summary_payload_lists_failed_feed_urls(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        aggregation = AggregationResult(
            successes=[],
            failures=[SourceAggregationFailure(source=source, error="boom")],
        )
        processed = ProcessedOutput(items=[], updated="2026-03-13 12:00:00")
        report = build_task_report(aggregation, processed, output_written=False, duration_seconds=1.25)

        payload = build_summary_payload(
            report=report,
            output_path=None,
            failure_log_path="data/failures.json",
        )

        self.assertEqual(["https://example.com/feed.xml"], payload["failed_feed_urls"])
        self.assertEqual("data/failures.json", payload["failure_log_path"])
        self.assertNotIn("format", payload)
        self.assertEqual("failure", payload["outcome"])

    def test_run_aggregation_returns_successful_result(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        item = ProcessedItem(
            title="Post",
            link="https://example.com/post",
            published="2026-03-13 10:00:00",
            name="Example",
            avatar=None,
        )
        aggregation = AggregationResult(successes=[RawFeedDocument(source=source, title="Feed", entries=[])])
        processed_output = ProcessedOutput(items=[item], updated="2026-03-13 12:00:00")

        with patch("feeds_aggregator.application.load_sources", return_value=InputLoadResult(format_name="txt", sources=[source])), \
             patch("feeds_aggregator.application.process_sources_to_items", return_value=(aggregation, [item])), \
             patch("feeds_aggregator.application.build_processed_output", return_value=processed_output), \
             patch("feeds_aggregator.application.write_output_file", return_value=Path("data/feeds.json")):
            result = run_aggregation(
                RunAggregationRequest(
                    sources_path="data/rss.txt",
                    output_path="data/feeds.json",
                    workers=2,
                    timeout_seconds=15.0,
                    max_items_per_source=10,
                    max_total_items=0,
                    max_days=180,
                    timezone_name="UTC",
                )
            )

        self.assertEqual("success", result.report.outcome)
        self.assertEqual("data/feeds.json", result.output_path)
        self.assertIsNone(result.output_error)
        self.assertEqual(1, result.report.output_items)

    def test_run_aggregation_preserves_output_error(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        item = ProcessedItem(
            title="Post",
            link="https://example.com/post",
            published="2026-03-13 10:00:00",
            name="Example",
            avatar=None,
        )
        aggregation = AggregationResult(successes=[RawFeedDocument(source=source, title="Feed", entries=[])])
        processed_output = ProcessedOutput(items=[item], updated="2026-03-13 12:00:00")

        with patch("feeds_aggregator.application.load_sources", return_value=InputLoadResult(format_name="txt", sources=[source])), \
             patch("feeds_aggregator.application.process_sources_to_items", return_value=(aggregation, [item])), \
             patch("feeds_aggregator.application.build_processed_output", return_value=processed_output), \
             patch("feeds_aggregator.application.write_output_file", side_effect=OSError("disk full")):
            result = run_aggregation(
                RunAggregationRequest(
                    sources_path="data/rss.txt",
                    output_path="data/feeds.json",
                    workers=2,
                    timeout_seconds=15.0,
                    max_items_per_source=10,
                    max_total_items=0,
                    max_days=180,
                    timezone_name="UTC",
                )
            )

        self.assertEqual("failure", result.report.outcome)
        self.assertIsNone(result.output_path)
        self.assertEqual("disk full", result.output_error)

    def test_run_aggregation_writes_failure_log_when_configured(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        failure = SourceAggregationFailure(source=source, error="boom")
        aggregation = AggregationResult(failures=[failure])
        processed_output = ProcessedOutput(items=[], updated="2026-03-13 12:00:00")

        with patch("feeds_aggregator.application.load_sources", return_value=InputLoadResult(format_name="txt", sources=[source])), \
             patch("feeds_aggregator.application.process_sources_to_items", return_value=(aggregation, [])), \
             patch("feeds_aggregator.application.build_processed_output", return_value=processed_output), \
             patch("feeds_aggregator.application.write_failure_log", return_value=Path("data/failures.json")) as mocked_failure_log:
            result = run_aggregation(
                RunAggregationRequest(
                    sources_path="data/rss.txt",
                    output_path="data/feeds.json",
                    workers=2,
                    timeout_seconds=15.0,
                    max_items_per_source=10,
                    max_total_items=0,
                    max_days=180,
                    timezone_name="UTC",
                    failure_log_path="data/failures.json",
                )
            )

        self.assertEqual("failure", result.report.outcome)
        self.assertEqual("data/failures.json", result.failure_log_path)
        self.assertIsNone(result.failure_log_error)
        mocked_failure_log.assert_called_once()

    def test_run_aggregation_preserves_failure_log_error(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        failure = SourceAggregationFailure(source=source, error="boom")
        aggregation = AggregationResult(failures=[failure])
        processed_output = ProcessedOutput(items=[], updated="2026-03-13 12:00:00")

        with patch("feeds_aggregator.application.load_sources", return_value=InputLoadResult(format_name="txt", sources=[source])), \
             patch("feeds_aggregator.application.process_sources_to_items", return_value=(aggregation, [])), \
             patch("feeds_aggregator.application.build_processed_output", return_value=processed_output), \
             patch("feeds_aggregator.application.write_failure_log", side_effect=OSError("readonly fs")):
            result = run_aggregation(
                RunAggregationRequest(
                    sources_path="data/rss.txt",
                    output_path="data/feeds.json",
                    workers=2,
                    timeout_seconds=15.0,
                    max_items_per_source=10,
                    max_total_items=0,
                    max_days=180,
                    timezone_name="UTC",
                    failure_log_path="data/failures.json",
                )
            )

        self.assertIsNone(result.failure_log_path)
        self.assertEqual("readonly fs", result.failure_log_error)

    def test_run_aggregation_propagates_input_validation_error(self):
        with patch("feeds_aggregator.application.load_sources", side_effect=InputValidationError("bad input")):
            with self.assertRaises(InputValidationError):
                run_aggregation(
                    RunAggregationRequest(
                        sources_path="data/rss.txt",
                        output_path="data/feeds.json",
                        workers=2,
                        timeout_seconds=15.0,
                        max_items_per_source=10,
                        max_total_items=0,
                        max_days=180,
                        timezone_name="UTC",
                    )
                )

    def test_run_aggregation_validate_only_skips_processing_and_output(self):
        source = FeedSource(source_url="https://example.com/feed.xml")

        with patch("feeds_aggregator.application.load_sources", return_value=InputLoadResult(format_name="txt", sources=[source])), \
             patch("feeds_aggregator.application.process_sources_to_items") as mocked_process, \
             patch("feeds_aggregator.application.write_output_file") as mocked_write:
            result = run_aggregation(
                RunAggregationRequest(
                    sources_path="data/rss.txt",
                    output_path="data/feeds.json",
                    workers=2,
                    timeout_seconds=15.0,
                    max_items_per_source=10,
                    max_total_items=0,
                    max_days=180,
                    timezone_name="UTC",
                    validate_only=True,
                )
            )

        self.assertTrue(result.validated_only)
        self.assertEqual("success", result.report.outcome)
        self.assertEqual(1, result.report.total_sources)
        self.assertEqual([], result.processed.items)
        self.assertIsNone(result.output_path)
        mocked_process.assert_not_called()
        mocked_write.assert_not_called()

    def test_main_returns_success_and_prints_summary_payload(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        document = RawFeedDocument(source=source, title="Feed", entries=[])
        item = ProcessedItem(
            title="Post",
            link="https://example.com/post",
            published="2026-03-13 10:00:00",
            name="Example",
            avatar=None,
        )
        written_output = ProcessedOutput(items=[item], updated="2026-03-13 12:00:00")
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        report = build_task_report(AggregationResult(successes=[document]), written_output, output_written=True, duration_seconds=0.1)

        with patch("sys.argv", ["feeds-aggregator", "--sources", "data/rss.txt", "--output", "data/feeds.json"]), \
             patch("sys.stdout", stdout_buffer), \
             patch("sys.stderr", stderr_buffer), \
             patch(
                 "feeds_aggregator.cli.run_aggregation",
                 return_value=RunAggregationResult(
                    report=report,
                     processed=written_output,
                     output_path="data/feeds.json",
                     aggregation=AggregationResult(successes=[document]),
                     failure_log_path=None,
                    validated_only=False,
                 ),
             ):
            exit_code = main()

        self.assertEqual(SUCCESS_EXIT_CODE, exit_code)
        payload = json.loads(stdout_buffer.getvalue())
        self.assertEqual("success", payload["outcome"])
        self.assertEqual("data/feeds.json", payload["output_path"])
        self.assertEqual(1, payload["output_items"])
        self.assertFalse(payload["validated_only"])
        self.assertEqual("", stderr_buffer.getvalue())

    def test_main_returns_input_error_for_invalid_sources(self):
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()

        with patch("sys.argv", ["feeds-aggregator", "--sources", "data/missing.txt"]), \
             patch("sys.stdout", stdout_buffer), \
             patch("sys.stderr", stderr_buffer), \
             patch("feeds_aggregator.cli.run_aggregation", side_effect=InputValidationError("bad input")):
            exit_code = main()

        self.assertEqual(INPUT_ERROR_EXIT_CODE, exit_code)
        self.assertEqual("", stdout_buffer.getvalue())
        self.assertIn("input error: bad input", stderr_buffer.getvalue())

    def test_main_returns_success_for_validate_only(self):
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        report = TaskReport(
            outcome="success",
            total_sources=1,
            successful_sources=0,
            failed_sources=0,
            output_items=0,
            duration_seconds=0.1,
            failures=[],
        )

        with patch("sys.argv", ["feeds-aggregator", "--sources", "data/rss.txt", "--validate-only"]), \
             patch("sys.stdout", stdout_buffer), \
             patch("sys.stderr", stderr_buffer), \
             patch(
                 "feeds_aggregator.cli.run_aggregation",
                 return_value=RunAggregationResult(
                     report=report,
                    processed=ProcessedOutput(items=[], updated=""),
                    output_path=None,
                    aggregation=AggregationResult(),
                    failure_log_path=None,
                    validated_only=True,
                ),
             ):
            exit_code = main()

        self.assertEqual(SUCCESS_EXIT_CODE, exit_code)
        payload = json.loads(stdout_buffer.getvalue())
        self.assertTrue(payload["validated_only"])
        self.assertIsNone(payload["output_path"])
        self.assertEqual("", stderr_buffer.getvalue())

    def test_main_returns_failure_when_output_write_fails(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        document = RawFeedDocument(source=source, title="Feed", entries=[])
        item = ProcessedItem(
            title="Post",
            link="https://example.com/post",
            published="2026-03-13 10:00:00",
            name="Example",
            avatar=None,
        )
        processed_output = ProcessedOutput(items=[item], updated="2026-03-13 12:00:00")
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        report = build_task_report(AggregationResult(successes=[document]), processed_output, output_written=False, duration_seconds=0.1)

        with patch("sys.argv", ["feeds-aggregator", "--sources", "data/rss.txt", "--output", "data/feeds.json"]), \
             patch("sys.stdout", stdout_buffer), \
             patch("sys.stderr", stderr_buffer), \
             patch(
                 "feeds_aggregator.cli.run_aggregation",
                 return_value=RunAggregationResult(
                     report=report,
                     processed=processed_output,
                     output_path=None,
                     aggregation=AggregationResult(successes=[document]),
                     output_error="disk full",
                     failure_log_path="data/failures.json",
                     failure_log_error="readonly fs",
                     validated_only=False,
                 ),
             ):
            exit_code = main()

        self.assertEqual(FAILURE_EXIT_CODE, exit_code)
        payload = json.loads(stdout_buffer.getvalue())
        self.assertEqual("failure", payload["outcome"])
        self.assertIsNone(payload["output_path"])
        self.assertEqual("data/failures.json", payload["failure_log_path"])
        self.assertFalse(payload["validated_only"])
        self.assertIn("output error: disk full", stderr_buffer.getvalue())
        self.assertIn("failure log error: readonly fs", stderr_buffer.getvalue())

    def test_process_sources_to_items_stops_submitting_sources_when_total_limit_reached(self):
        sources = [
            FeedSource(source_url="https://example.com/feed-a.xml"),
            FeedSource(source_url="https://example.com/feed-b.xml"),
        ]
        document = RawFeedDocument(source=sources[0], title="Feed A", entries=[])
        item = ProcessedItem(
            title="Post",
            link="https://example.com/post",
            published="2026-03-13 10:00:00",
            name="Example",
            avatar=None,
            feed_domain="example.com",
            source_key="https://example.com/feed-a.xml",
        )

        with patch("feeds_aggregator.runner.process_single_source", return_value=(document, [item])) as mocked_process:
            aggregation, source_items = process_sources_to_items(
                sources,
                aggregation_config=AggregationConfig(workers=1),
                processing_config=ProcessingConfig(max_total_items=1),
                output_path="data/feeds.json",
                avatar_dir=None,
            )

        self.assertEqual(1, mocked_process.call_count)
        self.assertEqual(1, len(aggregation.successes))
        self.assertEqual(1, len(source_items))

    def test_persist_avatars_saves_favicon_file_with_expected_name(self):
        avatar_url = "https://cdn.example.com/images/logo.png?size=64"
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    avatar=avatar_url,
                    feed_domain="feeds.example.com",
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "feeds.json"
            with patch("feeds_aggregator.output_writer.urlopen", return_value=MockHttpResponse(b"PNGDATA")):
                persisted = persist_avatars(output, output_path=output_path, timeout_seconds=1.0)

            avatar_hash = hashlib.sha256(avatar_url.encode("utf-8")).hexdigest()[:16]
            expected_name = f"feeds_example_com_{avatar_hash}.png"
            expected_file = output_path.parent / "favicons" / expected_name

            self.assertEqual(expected_name, persisted.items[0].avatar)
            self.assertTrue(expected_file.exists())
            self.assertEqual(b"PNGDATA", expected_file.read_bytes())

    def test_persist_avatars_supports_custom_dir(self):
        avatar_url = "https://img.example.org/icons/site.ico"
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    avatar=avatar_url,
                    feed_domain="feeds.example.com",
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "data" / "feeds.json"
            avatar_dir = Path(tmpdir) / "custom_avatars"
            with patch("feeds_aggregator.output_writer.urlopen", return_value=MockHttpResponse(b"ICODATA", content_type="image/x-icon")):
                persisted = persist_avatars(
                    output,
                    output_path=output_path,
                    avatar_dir=avatar_dir,
                    timeout_seconds=1.0,
                )

            avatar_hash = hashlib.sha256(avatar_url.encode("utf-8")).hexdigest()[:16]
            expected_name = f"feeds_example_com_{avatar_hash}.ico"
            expected_file = avatar_dir / expected_name

            self.assertEqual(expected_name, persisted.items[0].avatar)
            self.assertTrue(expected_file.exists())
            self.assertEqual(b"ICODATA", expected_file.read_bytes())

    def test_persist_avatars_reuses_existing_file_without_downloading(self):
        avatar_url = "https://cdn.example.com/images/logo.png?size=64"
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    avatar=avatar_url,
                    feed_domain="feeds.example.com",
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "feeds.json"
            avatar_dir = output_path.parent / "favicons"
            avatar_dir.mkdir(parents=True, exist_ok=True)
            avatar_hash = hashlib.sha256(avatar_url.encode("utf-8")).hexdigest()[:16]
            expected_name = f"feeds_example_com_{avatar_hash}.png"
            expected_file = avatar_dir / expected_name
            expected_file.write_bytes(b"EXISTING")

            with patch("feeds_aggregator.output_writer.urlopen") as mocked_urlopen:
                persisted = persist_avatars(output, output_path=output_path, timeout_seconds=1.0)

            self.assertEqual(expected_name, persisted.items[0].avatar)
            self.assertEqual(b"EXISTING", expected_file.read_bytes())
            mocked_urlopen.assert_not_called()

    def test_persist_avatars_keeps_remote_url_when_download_fails(self):
        avatar_url = "https://cdn.example.com/images/logo.png"
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    avatar=avatar_url,
                    feed_domain="feeds.example.com",
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "feeds.json"
            with patch("feeds_aggregator.output_writer.urlopen", side_effect=OSError("network down")):
                persisted = persist_avatars(output, output_path=output_path, timeout_seconds=1.0)

        self.assertEqual(avatar_url, persisted.items[0].avatar)

    def test_persist_avatars_discovers_icon_from_page_link(self):
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    avatar=None,
                    feed_domain="feeds.example.com",
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        html_response = MockHttpResponse(
            b"<html><head><link rel=\"icon\" href=\"/assets/icon.png\"></head></html>",
            content_type="text/html",
        )
        image_response = MockHttpResponse(b"PNGDATA", content_type="image/png")

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "feeds.json"
            with patch("feeds_aggregator.output_writer.urlopen", side_effect=[html_response, image_response]):
                persisted = persist_avatars(output, output_path=output_path, timeout_seconds=1.0)

            avatar_hash = hashlib.sha256("https://example.com/assets/icon.png".encode("utf-8")).hexdigest()[:16]
            expected_name = f"feeds_example_com_{avatar_hash}.png"

            self.assertEqual(expected_name, persisted.items[0].avatar)

    def test_persist_avatars_leaves_avatar_empty_when_page_has_no_icon(self):
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    avatar=None,
                    feed_domain="feeds.example.com",
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        html_response = MockHttpResponse(b"<html><head></head><body>No icon</body></html>", content_type="text/html")

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "feeds.json"
            with patch("feeds_aggregator.output_writer.urlopen", return_value=html_response):
                persisted = persist_avatars(output, output_path=output_path, timeout_seconds=1.0)

        self.assertIsNone(persisted.items[0].avatar)

    def test_persist_avatars_discovers_once_per_feed_source(self):
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post A",
                    link="https://example.com/post-a",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    avatar=None,
                    feed_domain="feeds.example.com",
                    source_key="https://feeds.example.com/feed.xml",
                ),
                ProcessedItem(
                    title="Post B",
                    link="https://example.com/post-b",
                    published="2026-03-13 09:00:00",
                    name="Example",
                    avatar=None,
                    feed_domain="feeds.example.com",
                    source_key="https://feeds.example.com/feed.xml",
                ),
            ],
            updated="2026-03-13 12:00:00",
        )

        html_response = MockHttpResponse(
            b"<html><head><link rel=\"icon\" href=\"/assets/icon.png\"></head></html>",
            content_type="text/html",
        )
        image_response = MockHttpResponse(b"PNGDATA", content_type="image/png")

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "feeds.json"
            with patch("feeds_aggregator.output_writer.urlopen", side_effect=[html_response, image_response]) as mocked_urlopen:
                persisted = persist_avatars(output, output_path=output_path, timeout_seconds=1.0)

        self.assertEqual(2, len(persisted.items))
        self.assertEqual(persisted.items[0].avatar, persisted.items[1].avatar)
        self.assertEqual(2, mocked_urlopen.call_count)

    def test_persist_avatars_uses_hostname_without_port_in_filename(self):
        avatar_url = "https://cdn.example.com:8443/images/logo.png?size=64"
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post",
                    link="https://example.com/post",
                    published="2026-03-13 10:00:00",
                    name="Example",
                    avatar=avatar_url,
                    feed_domain="feeds.example.com",
                )
            ],
            updated="2026-03-13 12:00:00",
        )

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "feeds.json"
            with patch("feeds_aggregator.output_writer.urlopen", return_value=MockHttpResponse(b"PNGDATA")):
                persisted = persist_avatars(output, output_path=output_path, timeout_seconds=1.0)

            avatar_hash = hashlib.sha256(avatar_url.encode("utf-8")).hexdigest()[:16]
            expected_name = f"feeds_example_com_{avatar_hash}.png"

            self.assertEqual(expected_name, persisted.items[0].avatar)

    def test_persist_avatars_keeps_feed_domains_distinct_for_same_avatar_url(self):
        avatar_url = "https://cdn.example.com/shared/logo.png"
        output = ProcessedOutput(
            items=[
                ProcessedItem(
                    title="Post A",
                    link="https://example.com/post-a",
                    published="2026-03-13 10:00:00",
                    name="Example A",
                    avatar=avatar_url,
                    feed_domain="feed-a.example.com",
                ),
                ProcessedItem(
                    title="Post B",
                    link="https://example.com/post-b",
                    published="2026-03-13 10:00:00",
                    name="Example B",
                    avatar=avatar_url,
                    feed_domain="feed-b.example.com",
                ),
            ],
            updated="2026-03-13 12:00:00",
        )

        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "feeds.json"
            with patch("feeds_aggregator.output_writer.urlopen", return_value=MockHttpResponse(b"PNGDATA")):
                persisted = persist_avatars(output, output_path=output_path, timeout_seconds=1.0)

            avatar_hash = hashlib.sha256(avatar_url.encode("utf-8")).hexdigest()[:16]
            self.assertEqual(f"feed-a_example_com_{avatar_hash}.png", persisted.items[0].avatar)
            self.assertEqual(f"feed-b_example_com_{avatar_hash}.png", persisted.items[1].avatar)

    def test_build_avatar_filename_replaces_commas_with_underscores(self):
        avatar_url = "https://cdn.example.com/logo.png"
        avatar_hash = hashlib.sha256(avatar_url.encode("utf-8")).hexdigest()[:16]

        filename = build_avatar_filename("cdn,example.com", avatar_url, ".png")

        self.assertEqual(f"cdn_example_com_{avatar_hash}.png", filename)

    def test_write_failure_log_creates_expected_json(self):
        source = FeedSource(source_url="https://example.com/feed.xml", category="tech", source_name="Example")
        failures = [SourceAggregationFailure(source=source, error="boom")]

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "logs" / "failures.json"
            written = write_failure_log(failures, path)
            payload = json.loads(written.read_text(encoding="utf-8"))

        self.assertEqual(1, len(payload))
        self.assertEqual("boom", payload[0]["error"])

    def test_build_task_report_returns_partial_success(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        aggregation = AggregationResult(
            successes=[RawFeedDocument(source=source, title="Feed", entries=[RawFeedEntry(title="Post", link="https://example.com/post", published="2026-03-13T10:00:00Z")])],
            failures=[SourceAggregationFailure(source=source, error="boom")],
        )
        processed = ProcessedOutput(items=[], updated="2026-03-13 12:00:00")

        report = build_task_report(aggregation, processed, output_written=True, duration_seconds=1.25)

        self.assertEqual("partial_success", report.outcome)
        self.assertEqual(1, report.successful_sources)
        self.assertEqual(1, report.failed_sources)

    def test_build_task_report_returns_failure_when_output_not_written(self):
        source = FeedSource(source_url="https://example.com/feed.xml")
        aggregation = AggregationResult(
            successes=[RawFeedDocument(source=source, title="Feed", entries=[RawFeedEntry(title="Post", link="https://example.com/post", published="2026-03-13T10:00:00Z")])],
            failures=[],
        )
        processed = ProcessedOutput(items=[], updated="2026-03-13 12:00:00")

        report = build_task_report(aggregation, processed, output_written=False, duration_seconds=1.25)

        self.assertEqual("failure", report.outcome)


if __name__ == "__main__":
    unittest.main()
