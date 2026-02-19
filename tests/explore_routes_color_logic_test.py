import CMroutes.explore_routes as explore_routes


def test_apply_node_colors_prefers_subdomain_over_group_label():
    rows = [{"labels": ["CATEGORY", "LANGUAGE", "LANGUOID"]}]
    label_metadata = {
        "LANGUAGE": {"color": "#8c6d31", "groupLabel": "LANGUOID"},
        "LANGUOID": {"color": "#843c39", "groupLabel": "LANGUOID"},
    }

    explore_routes._apply_node_colors(rows, label_metadata)

    assert rows[0]["legendLabel"] == "LANGUAGE"
    assert rows[0]["color"] == "#8c6d31"


def test_apply_node_colors_keeps_group_when_no_subdomain_present():
    rows = [{"labels": ["CATEGORY", "LANGUOID"]}]
    label_metadata = {
        "LANGUOID": {"color": "#843c39", "groupLabel": "LANGUOID"},
    }

    explore_routes._apply_node_colors(rows, label_metadata)

    assert rows[0]["legendLabel"] == "LANGUOID"
    assert rows[0]["color"] == "#843c39"


def test_apply_node_colors_averages_distinct_top_level_labels():
    rows = [{"labels": ["CATEGORY", "ADM0", "ADM1"]}]
    label_metadata = {
        "ADM0": {"color": "#d62728", "groupLabel": "ADM0"},
        "ADM1": {"color": "#2ca02c", "groupLabel": "ADM1"},
    }

    expected = explore_routes._desaturate_hex(
        explore_routes._average_hex(["#d62728", "#2ca02c"])
    )

    explore_routes._apply_node_colors(rows, label_metadata)

    assert rows[0]["legendLabel"] == "ADM0:ADM1"
    assert rows[0]["color"] == expected


def test_apply_node_colors_preserves_single_color_when_identical():
    rows = [{"labels": ["CATEGORY", "A", "B"]}]
    label_metadata = {
        "A": {"color": "#123456", "groupLabel": "A"},
        "B": {"color": "#123456", "groupLabel": "B"},
    }

    explore_routes._apply_node_colors(rows, label_metadata)

    assert rows[0]["legendLabel"] == "A:B"
    assert rows[0]["color"] == "#123456"
