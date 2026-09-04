from CM import routines
from CM.routines import _format_routine_email_cell


def test_routine_email_cell_highlights_zero_green_and_values_above_one_red():
    assert "color: #000000;" in _format_routine_email_cell("Check Domains", 0)
    assert "background-color: #c6efce;" in _format_routine_email_cell("Check Domains", 0)
    assert "background-color: #ffc7ce;" in _format_routine_email_cell("Check Domains", "2")


def test_modifications_routine_is_never_highlighted():
    for value in (0, 2):
        cell = _format_routine_email_cell("Modifications", value)
        assert "color: #000000;" in cell
        assert "background-color" not in cell


def test_non_numeric_routine_email_cell_remains_unhighlighted():
    cell = _format_routine_email_cell("Check Domains", "Exception: unavailable")
    assert cell == '<td style="color: #000000;">Exception: unavailable</td>'


def test_report_changes_default_includes_moved_relationship(monkeypatch):
    captured = {}

    monkeypatch.setattr(routines, "getDriver", lambda database: object())

    def fake_get_query(query, driver, params=None, type=None):
        captured["query"] = query
        return []

    monkeypatch.setattr(routines, "getQuery", fake_get_query)

    result = routines.reportChanges("ArchaMap", return_type="info")

    assert "moved relationship" in captured["query"]
    assert result["filepath"] is None
