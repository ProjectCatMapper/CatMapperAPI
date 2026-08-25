# CatMapper API

Flask API and background workers for [CatMapper](https://catmapper.org), a platform for connecting and exploring archaeological and social-science datasets. This repository contains the HTTP routes, Neo4j data operations, upload and reconciliation jobs, authentication, linked-data publication, and operational routines.

## Repository map

- `app.py` creates the Flask application and registers route blueprints.
- `CMroutes/` contains HTTP endpoints, authentication checks, and queue adapters.
- `CM/` contains database, upload, search, merge, reconciliation, and linked-data logic.
- `tests/` contains unit, route, integration, and opt-in real-Neo4j tests.
- `swagger.yml` and `/docs` describe the HTTP API.
- `Dockerfile` and `uwsgi.ini` define the production service image.
- `TEST_OVERVIEW.md` documents test groups and real-database gates.

## Requirements

- Python 3.13 in production; local tests use the `api_env` Conda environment.
- Neo4j and Redis for database-backed and queued workflows.
- Runtime configuration supplied through environment variables or a local `.env`; never commit credentials.

Copy `.env.example` to `.env` only for local development and fill in values appropriate to your environment. The committed example contains names and safe placeholders, not working secrets.

## Run locally

Install dependencies into an isolated environment, then start Flask directly:

```bash
python -m pip install -r requirements.txt
python app.py
```

The containerized production entrypoint uses uWSGI. From the CatMapper superproject, Docker Compose provides the API, Redis, workers, Neo4j services, and nginx integration.

## Tests

Run the default suite from this repository:

```bash
conda run -n api_env python -m pytest -q
```

Tests marked `realdb` are skipped by default. They must only be run intentionally against a disposable target:

```bash
conda run -n api_env python -m pytest -q -m realdb --run-realdb
```

See `TEST_OVERVIEW.md` for suite scope and database-selection options.

## API and data contracts

- Public and authenticated routes are registered in `CMroutes/`.
- Existing endpoints are also exposed beneath `/api` while canonical REST paths are adopted.
- `/health` reports service health and the committed application version.
- `/docs` serves Swagger UI and `/swagger` serves the OpenAPI source.
- Linked-data routes publish RDF, JSON-LD, Turtle, ontology, context, and reconciliation resources.

Changes to upload, merge, USES, or reconciliation behavior should include focused regression tests. Preserve relationship-level output contracts where multiple USES ties may share a node.

## Commit versions and deployment

Run the repository hook installer from the superproject, or configure this checkout directly:

```bash
git config core.hooksPath .githooks
```

The pre-commit hook writes a timestamped version to `VERSION`, so every commit identifies the exact application version it contains. Deployment scripts consume that committed value and tag the existing commit; they do not create a separate version-only commit.

Production deployment is orchestrated by the superproject's `deploy_production.sh`. Do not deploy from an uncommitted or non-fast-forwardable checkout.

## Security

- Do not commit `.env`, API keys, passwords, private keys, database credentials, or generated credential files.
- The repository-managed pre-commit hook scans staged paths and additions for likely secrets.
- See `SECURITY_PUBLIC_READINESS.md` for repository-setting recommendations.

Security issues should be reported privately to the CatMapper maintainers rather than opened with sensitive details in a public issue.
