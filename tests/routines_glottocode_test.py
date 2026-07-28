import pandas as pd

from CM import routines


def test_getBadGlottocodes_reports_property_and_uses_errors(monkeypatch, tmp_path):
    captured = {}
    rows = pd.DataFrame(
        [
            {
                "errorType": "Invalid LANGUOID glottocode property",
                "datasetID": None,
                "datasetName": None,
                "CMID": "SM1",
                "CMName": "Bad language",
                "Key": None,
                "nodeGlottocode": '"abcd123"',
                "keyGlottocode": None,
            },
            {
                "errorType": "Glottocode USES Key does not match LANGUOID property",
                "datasetID": "SD1",
                "datasetName": "Languages",
                "CMID": "SM2",
                "CMName": "Other language",
                "Key": "glottocode == wxyz1234",
                "nodeGlottocode": '"abcd1234"',
                "keyGlottocode": "wxyz1234",
            },
        ]
    )

    monkeypatch.setattr(routines, "getDriver", lambda database: object())

    def fake_get_query(query, driver, type=None, **kwargs):
        captured["query"] = query
        captured["type"] = type
        return rows

    monkeypatch.setattr(routines, "getQuery", fake_get_query)
    report_path = tmp_path / "invalid_glottocodes.xlsx"
    monkeypatch.setattr(
        routines.tempfile,
        "NamedTemporaryFile",
        lambda **kwargs: open(report_path, "wb"),
    )

    result = routines.getBadGlottocodes("SocioMap", return_type="data")

    assert result["Total"] == 2
    assert result["Invalid LANGUOID properties"] == 1
    assert result["Invalid or mismatched USES keys"] == 1
    assert len(result["Glottocode errors"]) == 2
    assert captured["type"] == "df"
    assert "size(stringGlottocode) <> 8" in captured["query"]
    assert "right(stringGlottocode, 4) =~ '^[0-9]{4}$'" in captured["query"]
    assert "stringNodeGlottocode <> keyGlottocode" in captured["query"]
    assert "split(r.Key, ' && ')" in captured["query"]


def test_getBadGlottocodes_info_creates_attachment(monkeypatch, tmp_path):
    monkeypatch.setattr(routines, "getDriver", lambda database: object())
    monkeypatch.setattr(
        routines,
        "getQuery",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "errorType": "Invalid glottocode USES Key",
                    "datasetID": "SD1",
                    "datasetName": "Languages",
                    "CMID": "SM1",
                    "CMName": "Language",
                    "Key": "glottocode == abcd12",
                    "nodeGlottocode": '"abcd1234"',
                    "keyGlottocode": "abcd12",
                }
            ]
        ),
    )
    report_path = tmp_path / "invalid_glottocodes.xlsx"
    monkeypatch.setattr(
        routines.tempfile,
        "NamedTemporaryFile",
        lambda **kwargs: open(report_path, "wb"),
    )

    result = routines.getBadGlottocodes("sociomap", return_type="info")

    assert result == {
        "info": (
            "Total: 1; Invalid LANGUOID properties: 0; "
            "Invalid or mismatched USES keys: 1"
        ),
        "filepath": str(report_path),
    }
    assert report_path.exists()


def test_getBadGlottocodes_is_sociomap_only(monkeypatch):
    monkeypatch.setattr(
        routines,
        "getDriver",
        lambda database: (_ for _ in ()).throw(AssertionError("driver should not be used")),
    )

    result = routines.getBadGlottocodes("ArchaMap", return_type="info")

    assert result == {
        "info": "Not applicable (SocioMap only)",
        "filepath": None,
    }
