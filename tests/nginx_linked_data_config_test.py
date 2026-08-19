from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_dev_nginx_has_explicit_ontology_and_negotiation_routes():
    config = (ROOT / "conf" / "nginx.conf").read_text(encoding="utf-8")
    dev_start = config.index("server_name dev.catmapper.org;")
    dev_end = config.index("server_name dev-api.catmapper.org;", dev_start)
    dev = config[dev_start:dev_end]

    assert "location = /ontology/catmapper" in dev
    assert "versions/$1.ttl" in dev
    assert "location /ontology/catmapper/" in dev
    assert "return 404" in dev
    assert "location = /contexts/catmapper" in dev
    assert "location = /schema/catmapper" in dev
    assert "@dev_linked_data" in dev
    assert "uwsgi_pass api-dev:5000" in dev
    assert "try_files $uri $uri/ /index.html" in dev


def test_production_canonical_server_publishes_ontology_and_negotiates_rdf():
    config = (ROOT / "conf" / "nginx.conf").read_text(encoding="utf-8")
    production_start = config.index("server_name catmapper.org www.catmapper.org;")
    production_end = config.index("server_name api.catmapper.org;", production_start)
    production = config[production_start:production_end]
    assert "location = /ontology/catmapper" in production
    assert "versions/$1.ttl" in production
    assert "location /ontology/catmapper/" in production
    assert "location = /contexts/catmapper" in production
    assert "location = /schema/catmapper" in production
    assert "location /exports/rdf/" in production
    assert "@prod_linked_data" in production
    assert "uwsgi_pass api:5000" in production
    assert "try_files $uri $uri/ /index.html" in production
