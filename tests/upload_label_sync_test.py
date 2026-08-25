import pytest

import CM.upload as upload


def test_sync_updated_uses_labels_reconciles_each_cmid_once(monkeypatch):
    calls = []
    monkeypatch.setattr(
        upload,
        "updateLabels",
        lambda **kwargs: calls.append(kwargs) or ["Set labels for 2 nodes"],
    )

    result = upload._sync_updated_uses_labels(
        "sociomap",
        ["SM21903", "SM21903", "SM42"],
        ["CMID", "datasetID", "Key", "label"],
    )

    assert result == ["Set labels for 2 nodes"]
    assert calls == [{"database": "sociomap", "CMID": ["SM21903", "SM42"]}]


def test_sync_updated_uses_labels_skips_non_label_edits(monkeypatch):
    monkeypatch.setattr(
        upload,
        "updateLabels",
        lambda **_kwargs: pytest.fail("updateLabels should not be called"),
    )

    assert upload._sync_updated_uses_labels(
        "sociomap", ["SM21903"], ["CMID", "Key", "Name"]
    ) is None


def test_sync_updated_uses_labels_propagates_reconciliation_failure(monkeypatch):
    monkeypatch.setattr(
        upload,
        "updateLabels",
        lambda **_kwargs: ("database error", 500),
    )

    with pytest.raises(RuntimeError, match="updateLabels failed: database error"):
        upload._sync_updated_uses_labels(
            "sociomap", ["SM21903"], ["label"]
        )
