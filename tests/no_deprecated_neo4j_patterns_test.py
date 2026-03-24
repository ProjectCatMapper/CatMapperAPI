from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT.parent

DEPRECATED_PATTERNS = [
    r"apoc\.when\(",
    r"apoc\.do\.when\(",
    r"apoc\.create\.setLabels\(",
    r"apoc\.meta\.cypher\.type\(",
    r"apoc\.meta\.cypher\.isType\(",
    r"apoc\.text\.levenshteinDistance\(",
]

SCAN_TARGETS = [
    REPO_ROOT / "CM",
    REPO_ROOT / "CMroutes",
]

CUSTOM_CYPHER_FILE = APP_ROOT / "db" / "import" / "customCypherFunctions.cypher"
if CUSTOM_CYPHER_FILE.exists():
    SCAN_TARGETS.append(CUSTOM_CYPHER_FILE)


def _iter_scan_files(target: Path):
    if target.is_file():
        yield target
        return

    for path in target.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in {".py", ".cypher"}:
            continue
        yield path


def test_no_deprecated_neo4j_patterns():
    violations = []

    for target in SCAN_TARGETS:
        for path in _iter_scan_files(target):
            text = path.read_text(encoding="utf-8", errors="ignore")
            for pattern in DEPRECATED_PATTERNS:
                for match in re.finditer(pattern, text):
                    line_number = text.count("\n", 0, match.start()) + 1
                    violations.append(f"{path}:{line_number}: {pattern}")

    assert not violations, (
        "Deprecated Neo4j/APOC patterns detected. Replace these before merge:\n"
        + "\n".join(violations)
    )
