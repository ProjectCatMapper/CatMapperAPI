from CM import metadata


def test_metadata_lookup_wrappers_do_not_retain_cross_request_results():
    cached_functions = (
        metadata.get_metadata_groups,
        metadata.get_public_subdomains,
        metadata.get_public_domains,
        metadata.get_domain_descriptions,
    )

    assert all(function.cache_info().maxsize == 0 for function in cached_functions)


def test_clear_metadata_caches_invalidates_all_cached_metadata_queries(monkeypatch):
    calls = []
    cached_functions = (
        "get_metadata_groups",
        "get_public_subdomains",
        "get_public_domains",
        "get_domain_descriptions",
    )

    for function_name in cached_functions:
        monkeypatch.setattr(
            getattr(metadata, function_name),
            "cache_clear",
            lambda name=function_name: calls.append(name),
        )

    metadata.clear_metadata_caches()

    assert calls == list(cached_functions)
