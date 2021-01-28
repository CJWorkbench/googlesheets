from googlesheets import migrate_params


def test_v0_with_file():
    assert migrate_params(
        {
            "has_header": False,
            "version_select": "",
            "googlefileselect": (
                '{"id":"1AR-sdfsdf","name":"Filename","url":"https://docs.goo'
                'gle.com/a/org/spreadsheets/d/1MJsdfwer/view?usp=drive_web","'
                'mimeType":"text/csv"}'
            ),
        }
    ) == {
        "has_header": False,
        "version_select": "",
        "file": {
            "id": "1AR-sdfsdf",
            "name": "Filename",
            "url": (
                "https://docs.google.com/a/org/spreadsheets/"
                "d/1MJsdfwer/view?usp=drive_web"
            ),
            "mimeType": "text/csv",
        },
    }


def test_v0_no_file():
    assert migrate_params(
        {"has_header": False, "version_select": "", "googlefileselect": ""}
    ) == {"has_header": False, "version_select": "", "file": None}


def test_v1():
    assert migrate_params(
        {
            "has_header": False,
            "version_select": "",
            "file": {
                "id": "1AR-sdfsdf",
                "name": "Filename",
                "url": (
                    "https://docs.google.com/a/org/spreadsheets/"
                    "d/1MJsdfwer/view?usp=drive_web"
                ),
                "mimeType": "text/csv",
            },
        }
    ) == {
        "has_header": False,
        "version_select": "",
        "file": {
            "id": "1AR-sdfsdf",
            "name": "Filename",
            "url": (
                "https://docs.google.com/a/org/spreadsheets/"
                "d/1MJsdfwer/view?usp=drive_web"
            ),
            "mimeType": "text/csv",
        },
    }
