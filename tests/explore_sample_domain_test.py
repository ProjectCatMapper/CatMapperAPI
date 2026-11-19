from CM.explore import _aggregate_samples


def test_aggregated_samples_keep_distinct_domains_for_their_uses_ties():
    base = {'Source': 'Example', 'link2': '/dataset', 'Location': 'Place'}
    samples = [
        {**base, 'Name': 'A', 'Key': 'one', 'Domain': ['LANGUAGE', 'ETHNICITY']},
        {**base, 'Name': 'B', 'Key': 'two', 'Domain': ['LANGUAGE']},
        {**base, 'Name': 'C', 'Key': 'three', 'Domain': None},
        {**base, 'Location': 'Other', 'Name': 'D', 'Domain': ['RELIGION']},
    ]

    rows = _aggregate_samples(samples)

    assert len(rows) == 2
    assert rows[0]['Domain'] == 'ETHNICITY, LANGUAGE'
    assert rows[0]['Key'] == 'one || two || three'
    assert rows[1]['Domain'] == 'RELIGION'


def test_aggregated_samples_leave_missing_domains_blank():
    assert _aggregate_samples([{'Source': 'Example'}])[0]['Domain'] == ''
