import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional

import cjwparquet
from cjwmodule.http import HttpError, httpfile
from cjwmodule.i18n import I18nMessage, trans
from cjwparse.api import MimeType, parse_file
from oauthlib import oauth2

GDRIVE_API_URL = "https://www.googleapis.com/drive/v3"  # unit tests override this


class RenderError(NamedTuple):
    """Mimics cjworkbench.cjwkernel.types.RenderError

    TODO move this to cjwmodule, so we can reuse it.
    """

    message: I18nMessage
    quick_fixes: List[None] = []


class FetchResult(NamedTuple):
    """Mimics cjworkbench.cjwkernel.types.FetchResult

    TODO move this to cjwmodule, so we can reuse it.
    """

    path: Path
    errors: List[RenderError] = []


def _generate_google_sheet_url(sheet_id: str) -> str:
    """
    URL to download text/csv from Google Drive.

    This uses the GDrive "export" API.

    Google Content-Type header is broken. According to RFC2616, "Data in
    character sets other than "ISO-8859-1" or its subsets MUST be labeled
    with an appropriate charset value". Google Sheets does not specify a
    charset (implying ISO-8859-1), but the text it serves is utf-8.

    So the caller should ignore the content-type Google returns.
    """
    return f"{GDRIVE_API_URL}/files/{sheet_id}/export?mimeType=text%2Fcsv"


def _generate_gdrive_file_url(sheet_id: str) -> str:
    """
    URL to download raw bytes from Google Drive.

    This discards Content-Type, including charset. GDrive doesn't know the
    charset anyway.
    """
    return f"{GDRIVE_API_URL}/files/{sheet_id}?alt=media"


def fetch_error(output_path: Path, message: I18nMessage):
    os.truncate(output_path, 0)
    return FetchResult(output_path, [RenderError(message)])


async def do_download(
    sheet_id: str, sheet_mime_type: str, oauth2_client: oauth2.Client, output_path: Path
) -> FetchResult:
    """
    Download spreadsheet from Google.

    If `sheet_mime_type` is 'application/vnd.google-apps.spreadsheet', use
    GDrive API to _export_ a text/csv. Otherwise, use GDrive API to _download_
    the file.
    """
    if sheet_mime_type == "application/vnd.google-apps.spreadsheet":
        url = _generate_google_sheet_url(sheet_id)
        sheet_mime_type = "text/csv"
    else:
        url = _generate_gdrive_file_url(sheet_id)
        # and use the passed sheet_mime_type

    url, headers, _ = oauth2_client.add_token(url, headers={})

    try:
        await httpfile.download(url, output_path, headers=headers)
    except HttpError.NotSuccess as err:
        response = err.response
        if response.status_code == 401:
            return fetch_error(
                output_path,
                trans(
                    "error.http.status401",
                    "Invalid credentials. Please reconnect to Google Drive.",
                ),
            )
        elif response.status_code == 403:
            return fetch_error(
                output_path,
                trans(
                    "error.http.status403",
                    "You chose a file your logged-in user cannot access. Please reconnect to Google Drive or choose a different file.",
                ),
            )
        elif response.status_code == 404:
            return fetch_error(
                output_path,
                trans(
                    "error.http.status404",
                    "File not found. Please choose a different file.",
                ),
            )
        else:
            return fetch_error(output_path, err.i18n_message)
    except HttpError as err:
        return fetch_error(output_path, err.i18n_message)

    return FetchResult(output_path)


def _render_deprecated_parquet(
    input_path: Path, errors: List[Any], output_path: Path, params: Dict[str, Any]
) -> List[I18nMessage]:
    cjwparquet.convert_parquet_file_to_arrow_file(input_path, output_path)
    if params["has_header"]:
        # In the deprecated parquet format, we _always_ parsed the header
        pass
    else:
        # We used to have a "moduleutils.turn_header_into_first_row()" but it
        # was broken by design (what about types???) and it was rarely used.
        # Let's not maintain it any longer.
        errors += [
            trans(
                "error.parquet.cannotRemoveHeader",
                "Please re-download this file to disable header-row handling",
            )
        ]

    return errors


def _calculate_mime_type(content_type: str) -> MimeType:
    for mime_type in MimeType:
        if content_type.startswith(mime_type.value):
            return mime_type
    # If we get here, we downloaded a MIME type we couldn't handle ... even
    # though we explicitly requested a MIME type we can handle. Undefined
    # behavior.
    raise RuntimeError("Unsupported content_type %s" % content_type)  # pragma: no cover


def _render_file(path: Path, params: Dict[str, Any], output_path: Path):
    with httpfile.read(path) as (parameters, status_line, headers, body_path):
        content_type = httpfile.extract_first_header(headers, "Content-Type")
        mime_type = _calculate_mime_type(content_type)
        # Ignore Google-reported charset. Google's headers imply latin-1 when
        # their data is utf-8.
        return parse_file(
            body_path,
            encoding=None,
            mime_type=mime_type,
            has_header=params["has_header"],
            output_path=output_path,
        )


def render(arrow_table, params, output_path, *, fetch_result, **kwargs):
    # Must perform header operation here in the event the header checkbox
    # state changes
    if fetch_result is None:
        # empty table
        return []
    elif fetch_result.path is not None and cjwparquet.file_has_parquet_magic_number(
        fetch_result.path
    ):
        # Deprecated files: we used to parse in fetch() and store the result
        # as Parquet. Now we've lost the original file data, and we need to
        # support our oldest users.
        #
        # In this deprecated format, parse errors were written as
        # fetch_result.errors.
        return _render_deprecated_parquet(
            fetch_result.path,
            [e.message for e in fetch_result.errors],
            output_path,
            params,
        )
    elif fetch_result.errors:
        # We've never stored errors+data. If there are errors, assume
        # there's no data.
        #
        # We've never stored errors with quick-fixes
        return [e.message for e in fetch_result.errors]
    else:
        assert not fetch_result.errors  # we've never stored errors+data.
        return _render_file(fetch_result.path, params, output_path)


def fetch_arrow(
    params: Dict[str, Any],
    secrets: Dict[str, Any],
    last_fetch_result: Any,
    input_table_parquet_path: Optional[Path],
    output_path: Path,
) -> FetchResult:
    file_meta = params["file"]
    if not file_meta:
        return fetch_error(
            output_path, trans("error.params.noFile", "Please choose a file")
        )

    # Ignore file_meta['url']. That's for the client's web browser, not for
    # an API request.
    sheet_id = file_meta["id"]
    if not sheet_id:
        # [adamhooper, 2019-12-06] has this ever happened?
        raise RuntimeError("Missing file.id")  # pragma: no cover

    # backwards-compat for old entries without 'mimeType', 2018-06-13
    sheet_mime_type = file_meta.get(
        "mimeType", "application/vnd.google-apps.spreadsheet"
    )

    secret = secrets.get("google_credentials")
    if not secret:
        return fetch_error(
            output_path,
            trans("error.secrets.noCredentials", "Please connect to Google Drive."),
        )
    if "error" in secret:
        return fetch_error(output_path, I18nMessage(**secret["error"]))
    assert "secret" in secret
    oauth2_client = oauth2.Client(
        client_id=None,  # unneeded
        token_type=secret["secret"]["token_type"],
        access_token=secret["secret"]["access_token"],
    )

    return asyncio.run(
        do_download(sheet_id, sheet_mime_type, oauth2_client, output_path)
    )


def _migrate_params_v0_to_v1(params):
    """
    v0: `googlefileselect` was a JSON-encoded String.

    v1: `file` is an Optional[Dict[str, str]]
    """
    if params["googlefileselect"]:
        file = json.loads(params["googlefileselect"])
    else:
        file = None
    return {
        "has_header": params["has_header"],
        "version_select": params["version_select"],
        "file": file,
    }


def migrate_params(params):
    if "googlefileselect" in params:
        params = _migrate_params_v0_to_v1(params)
    return params
