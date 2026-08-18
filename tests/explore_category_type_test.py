from CM import explore


def test_category_sample_query_returns_category_type_without_population_estimate():
    query = explore._get_queries_for_label("CATEGORY", database="SocioMap")["samples"]

    assert "WHEN cTypeCount >= 1 THEN r.categoryType" in query
    assert "r.populationEstimate IS NULL OR r.populationEstimate = 0" not in query
