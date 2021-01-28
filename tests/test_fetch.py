import contextlib
import tempfile
from pathlib import Path
from typing import Any, ContextManager, Dict, Optional

import httpx
from cjwmodule.http import httpfile
from cjwmodule.testing.i18n import cjwmodule_i18n_message, i18n_message
from pytest_httpx import HTTPXMock

from googlesheets import FetchResult, fetch_arrow

DEFAULT_SECRET = {
    "name": "x",
    "secret": {
        # As returned by fetcher.secrets.process_secret_oauth2()
        "token_type": "Bearer",
        "access_token": "an-access-token",
    },
}
DEFAULT_FILE = {
    "id": "aushwyhtbndh7365YHALsdfsdf987IBHJB98uc9uisdj",
    "name": "Police Data",
    "url": "http://example.org/police-data",
    "mimeType": "application/vnd.google-apps.spreadsheet",
}


def P(file=DEFAULT_FILE, has_header=True):
    return dict(file=file, has_header=has_header)


def secrets(secret: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if secret:
        return {"google_credentials": secret}
    else:
        return {}


@contextlib.contextmanager
def _fetch(
    params: Dict[str, Any], secrets: Dict[str, Any]
) -> ContextManager[FetchResult]:
    with tempfile.NamedTemporaryFile() as tf:
        output_path = Path(tf.name)
        yield fetch_arrow(params, secrets, None, None, output_path)


def test_fetch_nothing():
    with _fetch(P(file=None), {}) as (path, errors):
        assert path.read_bytes() == b""
        assert [e.message for e in errors] == [i18n_message("error.params.noFile")]


def test_fetch_native_sheet(httpx_mock: HTTPXMock):
    body = b"A,B\nx,y\nz,a"
    httpx_mock.add_response(
        url="https://www.googleapis.com/drive/v3/files/aushwyhtbndh7365YHALsdfsdf987IBHJB98uc9uisdj/export?mimeType=text%2Fcsv",
        data=body,
        headers={"Content-Type": "text/csv", "Content-Length": str(len(body))},
    )
    with _fetch(P(), secrets(DEFAULT_SECRET)) as (path, errors):
        assert errors == []
        with httpfile.read(path) as (_, __, headers, body_path):
            assert body_path.read_bytes() == body
            assert headers == [("Content-Type", "text/csv"), ("Content-Length", "11")]


def test_fetch_csv_file(httpx_mock: HTTPXMock):
    body = b"A,B\nx,y\nz,a"
    httpx_mock.add_response(
        url="https://www.googleapis.com/drive/v3/files/aushwyhtbndh7365YHALsdfsdf987IBHJB98uc9uisdj?alt=media",
        data=body,
        headers={"Content-Type": "text/csv", "Content-Length": str(len(body))},
    )
    with _fetch(
        P(file={**DEFAULT_FILE, "mimeType": "text/csv"}), secrets(DEFAULT_SECRET)
    ) as (path, errors):
        assert errors == []
        with httpfile.read(path) as (_, __, headers, body_path):
            assert body_path.read_bytes() == body
            assert headers == [("Content-Type", "text/csv"), ("Content-Length", "11")]


def test_fetch_tsv_file(httpx_mock: HTTPXMock):
    body = b"A\tB\nx\ty\nz\ta"
    httpx_mock.add_response(
        url="https://www.googleapis.com/drive/v3/files/aushwyhtbndh7365YHALsdfsdf987IBHJB98uc9uisdj?alt=media",
        data=body,
        headers={
            "Content-Type": "text/tab-separated-values",
            "Content-Length": str(len(body)),
        },
    )
    with _fetch(
        P(file={**DEFAULT_FILE, "mimeType": "text/tab-separated-values"}),
        secrets(DEFAULT_SECRET),
    ) as (path, errors):
        assert errors == []
        with httpfile.read(path) as (_, __, headers, body_path):
            assert body_path.read_bytes() == body
            assert headers == [
                ("Content-Type", "text/tab-separated-values"),
                ("Content-Length", "11"),
            ]


def test_fetch_xls_file(httpx_mock: HTTPXMock):
    body = b"abcd"
    httpx_mock.add_response(
        url="https://www.googleapis.com/drive/v3/files/aushwyhtbndh7365YHALsdfsdf987IBHJB98uc9uisdj?alt=media",
        data=body,
        headers={
            "Content-Type": "application/vnd.ms-excel",
            "Content-Length": str(len(body)),
        },
    )
    with _fetch(
        P(file={**DEFAULT_FILE, "mimeType": "application/vnd.ms-excel"}),
        secrets(DEFAULT_SECRET),
    ) as (path, errors):
        assert errors == []
        with httpfile.read(path) as (_, __, headers, body_path):
            assert body_path.read_bytes() == body
            assert headers == [
                ("Content-Type", "application/vnd.ms-excel"),
                ("Content-Length", "4"),
            ]


def test_fetch_xlsx_file(httpx_mock: HTTPXMock):
    body = b"abcd"
    httpx_mock.add_response(
        url="https://www.googleapis.com/drive/v3/files/aushwyhtbndh7365YHALsdfsdf987IBHJB98uc9uisdj?alt=media",
        data=body,
        headers={
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "Content-Length": str(len(body)),
        },
    )
    with _fetch(
        P(
            file={
                **DEFAULT_FILE,
                "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            }
        ),
        secrets(DEFAULT_SECRET),
    ) as (path, errors):
        assert errors == []
        with httpfile.read(path) as (_, __, headers, body_path):
            assert body_path.read_bytes() == body
            assert headers == [
                (
                    "Content-Type",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ),
                ("Content-Length", "4"),
            ]


def test_missing_secret_error():
    with _fetch(P(), {}) as (path, errors):
        assert path.read_bytes() == b""
        assert [e.message for e in errors] == [
            i18n_message("error.secrets.noCredentials")
        ]


def test_http_401_unauthorized(httpx_mock: HTTPXMock):
    httpx_mock.add_response(status_code=401)
    with _fetch(P(), secrets=secrets(DEFAULT_SECRET)) as (path, errors):
        assert path.read_bytes() == b""
        assert [e.message for e in errors] == [i18n_message("error.http.status401")]


def test_http_403_forbidden(httpx_mock: HTTPXMock):
    httpx_mock.add_response(status_code=403)
    with _fetch(P(), secrets=secrets(DEFAULT_SECRET)) as (path, errors):
        assert path.read_bytes() == b""
        assert [e.message for e in errors] == [i18n_message("error.http.status403")]


def test_http_404_not_found(httpx_mock: HTTPXMock):
    httpx_mock.add_response(status_code=404)
    with _fetch(P(), secrets=secrets(DEFAULT_SECRET)) as (path, errors):
        assert path.read_bytes() == b""
        assert [e.message for e in errors] == [i18n_message("error.http.status404")]


def test_http_not_200_ok_generic(httpx_mock: HTTPXMock):
    httpx_mock.add_response(status_code=429)
    with _fetch(P(), secrets=secrets(DEFAULT_SECRET)) as (path, errors):
        assert path.read_bytes() == b""
        assert [e.message for e in errors] == [
            cjwmodule_i18n_message(
                "http.errors.HttpErrorNotSuccess",
                {"status_code": 429, "reason": "Too Many Requests"},
            )
        ]


def test_unhandled_http_error(httpx_mock: HTTPXMock):
    def raise_timeout(request, ext):
        raise httpx.ReadTimeout("Gave up", request=request)

    httpx_mock.add_callback(raise_timeout)
    with _fetch(P(), secrets=secrets(DEFAULT_SECRET)) as (path, errors):
        assert path.read_bytes() == b""
        assert [e.message for e in errors] == [
            # googlesheet should pass through cjwmodule.http.client's message
            cjwmodule_i18n_message("http.errors.HttpErrorTimeout")
        ]


def test_secret_error():
    # Workbench runs oauth _before fetch_. It passes the error to fetch().
    with _fetch(
        P(),
        secrets=secrets(
            {
                "name": "x",
                "error": {
                    "id": "py.fetcher.secrets._refresh_oauth2_token.error.general",
                    "arguments": {
                        "status_code": 400,
                        "error": "invalid_grant",
                        "description": "Token has been expired or revoked.",
                    },
                    "source": None,
                },
            }
        ),
    ) as (path, errors):
        assert path.read_bytes() == b""
        assert [e.message for e in errors] == [
            (
                "py.fetcher.secrets._refresh_oauth2_token.error.general",
                {
                    "status_code": 400,
                    "error": "invalid_grant",
                    "description": "Token has been expired or revoked.",
                },
                None,
            )
        ]
