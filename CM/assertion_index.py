"""Persistent reverse lookup for public assertion hashes; Neo4j remains authoritative."""

import hashlib
import os
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path


class AssertionIndexUnavailable(RuntimeError):
    pass


def index_path():
    return Path(os.environ.get(
        "CATMAPPER_ASSERTION_INDEX",
        str(Path(__file__).resolve().parents[1] / "tmp" / "linked-data" / "assertions.sqlite3"),
    ))


def assertion_digest(database, dataset, key, concept):
    return hashlib.sha256("\0".join((database, dataset, key, concept)).encode("utf-8")).hexdigest()


@contextmanager
def assertion_index_writer():
    """Reuse the page cache during backfills, committing each small batch separately."""
    path = index_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path, timeout=5)) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA cache_size=-65536")
            with connection:
                connection.execute("""CREATE TABLE IF NOT EXISTS assertions (
                    database TEXT NOT NULL, digest TEXT NOT NULL,
                    dataset TEXT NOT NULL, source_key TEXT NOT NULL, concept TEXT NOT NULL,
                    PRIMARY KEY (database, digest)) WITHOUT ROWID""")
            yield connection
    except (OSError, sqlite3.Error) as exc:
        raise AssertionIndexUnavailable("Assertion lookup index is unavailable.") from exc


def remember_assertions(database, rows, *, connection=None):
    identities = {}
    for row in rows:
        parts = (database, str(row.get("datasetCmid") or ""),
                 str(row.get("key") or ""), str(row.get("conceptCmid") or ""))
        if any(not value.strip() for value in parts):
            continue
        digest = assertion_digest(*parts)
        identities[digest] = (database, digest, parts[1], parts[2], parts[3])
    if connection is None:
        with assertion_index_writer() as writer:
            return _remember_identities(database, identities, writer)
    return _remember_identities(database, identities, connection)


def _remember_identities(database, identities, connection):
    # Existing published identities need only reads, even during a backfill.
    digests = list(identities)
    for offset in range(0, len(digests), 500):
        batch = digests[offset:offset + 500]
        placeholders = ",".join("?" for _ in batch)
        known = connection.execute(
            f"SELECT digest FROM assertions WHERE database=? AND digest IN ({placeholders})",
            (database, *batch),
        )
        for (digest,) in known:
            identities.pop(digest, None)
    if identities:
        with connection:
            connection.executemany(
                "INSERT OR IGNORE INTO assertions VALUES (?, ?, ?, ?, ?)",
                sorted(identities.values()),
            )


def lookup_assertion(database, digest):
    path = index_path()
    try:
        # Read-only connections cannot accidentally create an empty index.
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
            row = connection.execute(
                "SELECT dataset, source_key, concept FROM assertions WHERE database=? AND digest=?",
                (database, digest),
            ).fetchone()
    except (OSError, sqlite3.Error) as exc:
        raise AssertionIndexUnavailable("Assertion lookup index is unavailable.") from exc
    if row is None:
        return None
    return dict(zip(("datasetCmid", "key", "conceptCmid"), row))


def backfill_complete(database, *, mark=False):
    """Record successful full scans, so routine deployments need not repeat them."""
    path = index_path()
    if not path.exists():
        if not mark:
            return False
        remember_assertions(database, [])
    with closing(sqlite3.connect(path, timeout=5)) as connection, connection:
        connection.execute("CREATE TABLE IF NOT EXISTS backfills (database TEXT PRIMARY KEY)")
        if mark:
            connection.execute("INSERT OR IGNORE INTO backfills VALUES (?)", (database,))
        return connection.execute("SELECT 1 FROM backfills WHERE database=?", (database,)).fetchone() is not None
