from CM.explore import _validate_points


def test_validate_points_preserves_category_identity_for_tooltips():
    points, bad_sources = _validate_points([{
        "geometry": '{"type":"Point","coordinates":[2,8]}',
        "source": "HRAF",
        "CMName": "Berber",
        "CMID": "SM250318",
    }])

    assert bad_sources == []
    assert points == [{
        "cood": [2, 8],
        "source": "HRAF",
        "CMName": "Berber",
        "CMID": "SM250318",
    }]
