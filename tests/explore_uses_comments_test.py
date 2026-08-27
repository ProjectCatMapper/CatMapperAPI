from CM import explore


def test_category_info_preserves_duplicate_non_empty_uses_comments():
    query = explore._get_queries_for_label("CATEGORY", database="SocioMap")["info"]

    assert "collect(trim(toString(r.comment)))" in query
    assert "collect(DISTINCT trim(toString(r.comment)))" not in query
    assert "comment IS NOT NULL AND comment <> ''" in query
