from CM import explore


def test_category_info_preserves_duplicate_non_empty_uses_comments():
    query = explore._get_queries_for_label("CATEGORY", database="SocioMap")["info"]

    assert "CASE WHEN r.comment IS NULL THEN [] ELSE [r.comment] END, true)" in query
    assert "collect(DISTINCT trim(toString(r.comment)))" not in query
    assert "apoc.coll.toSet" not in query
    assert "comment IS NOT NULL AND trim(toString(comment)) <> ''" in query
    assert "d.CMID + ': ' + trim(toString(comment))]), true) AS UsesComments" in query
