"""Stored Neo4j representations shared by maintenance and semantic auditing.

Adding a metatype requires an explicit storage rule here and a round-trip case in
metatype_repair_test.py. Numeric validation is separate: numeric source fields
whose metadata says STRING continue to be stored as strings.
"""

from types import MappingProxyType


METATYPE_STORAGE_TYPES = MappingProxyType({
    "STRING": frozenset({"STRING NOT NULL"}),
    # Neo4j gives an empty list its own valid storage representation.
    "LIST": frozenset({"LIST<STRING NOT NULL> NOT NULL", "LIST<NOTHING> NOT NULL"}),
    "BOOLEAN": frozenset({"BOOLEAN NOT NULL"}),
})


def normalize_metatype(meta_type):
    name = str(meta_type).strip().upper()
    if name not in METATYPE_STORAGE_TYPES:
        raise ValueError(f"Unsupported metatype {name!r}; add a storage rule and maintenance round-trip test")
    return name


def storage_types(meta_type):
    return METATYPE_STORAGE_TYPES[normalize_metatype(meta_type)]


def storage_predicate(value, meta_type):
    # Both the type names and representations come from the closed registry.
    representations = ", ".join(repr(t) for t in sorted(storage_types(meta_type)))
    return f"valueType({value}) IN [{representations}]"


def metadata_metatypes(metadata):
    """Validate all definitions before maintenance writes any field."""
    definitions = {"node": {}, "relationship": {}}
    for row in metadata:
        if row.get("metaType") is None:
            continue
        kind = row.get("type")
        prop = row.get("property")
        if kind not in definitions or not isinstance(prop, str) or not prop.strip() or "`" in prop:
            raise ValueError("Invalid property storage definition")
        meta_type = normalize_metatype(row["metaType"])
        previous = definitions[kind].setdefault(prop, meta_type)
        if previous != meta_type:
            raise ValueError(f"Conflicting metatypes for {kind} property {prop}")
    return definitions


STORAGE_SHAPE_QUERIES = {
    kind: f"""{match}
    UNWIND keys({alias}) AS key
    WITH {alias}, key WHERE key IN $properties
    RETURN key, collect(DISTINCT valueType({alias}[key])) AS types, count(*) AS count
    ORDER BY key"""
    for kind, match, alias in (
        ("node", "MATCH (n)", "n"),
        ("relationship", "MATCH ()-[r]->()", "r"),
    )
}


def verify_storage(driver, definitions, query_runner):
    """Read back every declared, present field after maintenance; never write."""
    issues = []
    evidence = {}
    for kind, fields in definitions.items():
        if not fields:
            continue
        rows = query_runner(STORAGE_SHAPE_QUERIES[kind], driver,
                            params={"properties": sorted(fields)}, type="dict")
        for row in rows:
            unexpected = set(row["types"]) - storage_types(fields[row["key"]])
            if unexpected:
                issues.append(f"{kind} property {row['key']} has unsupported storage types {sorted(unexpected)}; "
                              f"expected {sorted(storage_types(fields[row['key']]))}")
        evidence[kind] = {"declaredFields": len(fields), "observedFields": len(rows),
                          "checkedValues": sum(row["count"] for row in rows)}
    return issues, evidence
