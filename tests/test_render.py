import contextlib
import io
import tempfile
from pathlib import Path
from typing import Any, ContextManager, Dict, List, Optional, Tuple

import pyarrow as pa
from cjwmodule.http import httpfile
from cjwmodule.testing.i18n import i18n_message

from googlesheets import FetchResult, RenderError, render

DEFAULT_FILE = {
    "id": "aushwyhtbndh7365YHALsdfsdf987IBHJB98uc9uisdj",
    "name": "Police Data",
    "url": "http://example.org/police-data",
    "mimeType": "application/vnd.google-apps.spreadsheet",
}


def P(file=DEFAULT_FILE, has_header=True):
    return dict(file=file, has_header=has_header)


@contextlib.contextmanager
def _temp_parquet_file(table: pa.Table) -> ContextManager[Path]:
    with tempfile.NamedTemporaryFile() as tf:
        path = Path(tf.name)
        pa.parquet.write_table(table, path, version="2.0", compression="SNAPPY")
        yield path


@contextlib.contextmanager
def _temp_httpfile(
    url: str,
    status_line: str,
    body: bytes,
    headers: List[Tuple[str, str]] = [("Content-Type", "text/html; charset=utf-8")],
) -> ContextManager[Path]:
    with tempfile.NamedTemporaryFile() as tf:
        path = Path(tf.name)
        httpfile.write(path, {"url": url}, status_line, headers, io.BytesIO(body))
        yield path


def _assert_table_file(path: Path, expected: Optional[pa.Table]) -> None:
    if expected is None:
        assert path.stat().st_size == 0
        return
    else:
        assert path.stat().st_size > 0

    with pa.ipc.open_file(path) as f:
        actual = f.read_all()
    assert actual.column_names == expected.column_names
    for actual_column, expected_column in zip(
        actual.itercolumns(), expected.itercolumns()
    ):
        assert actual_column.type == expected_column.type
        assert actual_column.to_pylist() == expected_column.to_pylist()
        if pa.types.is_dictionary(actual_column.type):
            for output_chunk, expected_chunk in zip(
                actual_column.iterchunks(), expected_column.iterchunks()
            ):
                assert (
                    output_chunk.dictionary.to_pylist()
                    == expected_chunk.dictionary.to_pylist()
                )


@contextlib.contextmanager
def _render(params: Dict[str, Any], fetch_result: Optional[FetchResult]):
    with tempfile.NamedTemporaryFile() as empty_file:
        output_path = Path(empty_file.name)
        errors = render((), params, output_path, fetch_result=fetch_result)
        yield output_path, errors


def test_render_no_file():
    with _render(P(), None) as (path, errors):
        assert errors == []
        _assert_table_file(path, None)


def test_render_fetch_error():
    with tempfile.NamedTemporaryFile() as empty_file:
        fetch_errors = [RenderError(i18n_message("x", {"y": "z"}))]
        with _render(P(), FetchResult(Path(empty_file.name), fetch_errors)) as (
            path,
            errors,
        ):
            _assert_table_file(path, None)
            assert errors == [i18n_message("x", {"y": "z"})]


def test_render_deprecated_parquet():
    with _temp_parquet_file(pa.table({"A": [1, 2], "B": [3, 4]})) as fetched_path:
        with _render(P(), FetchResult(fetched_path)) as (path, errors):
            _assert_table_file(path, pa.table({"A": [1, 2], "B": [3, 4]}))
            assert errors == []


def test_render_deprecated_parquet_warning():
    with _temp_parquet_file(pa.table({"A": [1, 2], "B": [3, 4]})) as fetched_path:
        fetch_errors = [RenderError(i18n_message("truncated table"))]
        with _render(P(), FetchResult(fetched_path, fetch_errors)) as (path, errors):
            _assert_table_file(path, pa.table({"A": [1, 2], "B": [3, 4]}))
            assert errors == [i18n_message("truncated table")]


def test_render_deprecated_parquet_has_header_false():
    # Back in the day, we parsed during fetch. But has_header can change
    # between fetch and render. We were lazy, so we made fetch() follow the
    # most-common path: has_header=True. Then, in render(), we would "undo"
    # the change if has_header=False. This mangles the input data, but we
    # have no choice because we lost the input data. It was unwise. We have
    # abandoned supporting these files.
    with _temp_parquet_file(pa.table({"A": [1, 2], "B": [3, 4]})) as fetched_path:
        with _render(P(has_header=False), FetchResult(fetched_path)) as (path, errors):
            _assert_table_file(path, pa.table({"A": [1, 2], "B": [3, 4]}))
            assert errors == [i18n_message("error.parquet.cannotRemoveHeader")]


def test_render_has_header_true():
    with _temp_httpfile(
        "https://blah",
        "200 OK",
        b"A,B\na,b",
        headers=[("content-type", "text/csv")],
    ) as fetch_path:
        with _render(P(has_header=True), FetchResult(fetch_path)) as (path, errors):
            _assert_table_file(path, pa.table({"A": ["a"], "B": ["b"]}))
            assert errors == []


def test_render_has_header_false():
    with _temp_httpfile(
        "https://blah",
        "200 OK",
        b"1,2\n3,4",
        headers=[("content-type", "text/csv")],
    ) as fetch_path:
        with _render(P(has_header=False), FetchResult(fetch_path)) as (path, errors):
            _assert_table_file(
                path,
                pa.table(
                    {
                        "Column 1": pa.array([1, 3], pa.int8()),
                        "Column 2": pa.array([2, 4], pa.int8()),
                    }
                ),
            )
            assert errors == []
