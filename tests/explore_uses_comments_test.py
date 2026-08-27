from CM import explore


def test_category_info_preserves_duplicate_non_empty_uses_comments():
    query = explore._get_queries_for_label("CATEGORY", database="SocioMap")["info"]

    assert "apoc.coll.toSet(apoc.coll.flatten(collect(r.comment), true))" in query
    assert "collect(DISTINCT trim(toString(r.comment)))" not in query
    assert "comment IS NOT NULL AND trim(toString(comment)) <> ''" in query
    assert "trim(toString(comment))] AS UsesComments" in query
