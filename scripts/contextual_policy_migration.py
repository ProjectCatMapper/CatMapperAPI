#!/usr/bin/env python
"""Preview/install contextual metadata; consolidate obsolete ArchaMap links.

Other invalid source assertions are reported for review and are never changed.
Run from CatMapperAPI after loading runtime configuration without printing it.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from neo4j import GraphDatabase
from CM import context_policy
from CM.utils import config, getDriver


RULES = {
    "sociomap": {
        "AREA": ("AREA_OF", "ALL_EXCEPT", []),
        "LANGUOID": ("LANGUOID_OF", "ALL_EXCEPT", ["AREA", "LANGUOID"]),
        "RELIGION": ("RELIGION_OF", "ALL_EXCEPT", ["AREA", "LANGUOID", "RELIGION"]),
        "POLITY": ("POLITY_OF", "ALL_EXCEPT", ["AREA", "LANGUOID", "RELIGION", "POLITY"]),
        "OCCUPATION": ("OCCUPATION_OF", "ONLY", ["ETHNICITY"]),
        "VARIABLE": ("VARIABLE_OF", "ONLY", ["ATTRIBUTE"]),
    },
    "archamap": {
        "AREA": ("AREA_OF", "ALL_EXCEPT", []),
        "PERIOD": ("PERIOD_OF", "ALL_EXCEPT", ["AREA", "PERIOD"]),
        "CULTURE": ("CULTURE_OF", "ALL_EXCEPT", ["AREA", "PERIOD", "CULTURE"]),
        "VARIABLE": ("VARIABLE_OF", "ONLY", ["ATTRIBUTE"]),
    },
}
LEGACY = {"FAUNA_OF", "BOTANICAL_OF", "LABEL_OF", "PROJECTILE_POINT_OF", "SITE_OF"}
LABELS = "MATCH (l:LABEL) RETURN elementId(l) AS id,properties(l) AS properties ORDER BY l.CMID"
PROPERTIES = "MATCH (p:PROPERTY) RETURN properties(p) AS properties ORDER BY p.CMID"
SITE_EDGES = """MATCH (a:CATEGORY)-[old:SITE_OF]->(b:CATEGORY)
    OPTIONAL MATCH (a)-[r:AREA_OF]->(b)
    RETURN elementId(old) AS id,a.CMID AS source,b.CMID AS target,
    properties(old) AS properties,collect({id:elementId(r),properties:properties(r)}) AS replacements
    ORDER BY source,target"""
SUPPORT = """MATCH (site:CATEGORY)-[:SITE_OF]->(b:CATEGORY)
    MATCH (d:DATASET)-[u:USES]->(b)
    WHERE site.CMID IN apoc.coll.flatten([coalesce(u.district,[]),coalesce(u.country,[])],true)
    RETURN site.CMID AS site,b.CMID AS target,d.CMID AS datasetID,u.Key AS Key,
        d.shortName + ' Key: ' + u.Key AS referenceKey,elementId(u) AS id,properties(u) AS properties
    ORDER BY site,target,id"""


def desired_labels(rows, database):
    result = []
    for row in rows:
        p = dict(row["properties"])
        name, group = p.get("CMName"), p.get("groupLabel")
        if name == group and name not in context_policy.STRUCTURAL:
            relationship, mode, targets = RULES[database].get(name, (None, "NONE", []))
            p["contextualTargetMode"] = mode
            if targets:
                p["contextualTargetDomains"] = targets
            else:
                p.pop("contextualTargetDomains", None)
            if relationship:
                p["relationship"] = relationship
                p["contextualOrder"] = str(list(RULES[database]).index(name) + 1)
            else:
                p.pop("relationship", None)
                p.pop("contextualOrder", None)
        elif name != group:
            p.pop("relationship", None)
        result.append({"id": row["id"], "properties": p})
    return result


def parsed_policy(labels, properties):
    return context_policy.parse_policy([
        {**r["properties"], "label": r["properties"]["CMName"]} for r in labels
    ], [{**r["properties"], "property": r["properties"]["CMName"]}
        for r in properties if r["properties"].get("type") == "relationship"])


def verify_site_provenance(edges, support):
    for edge in edges:
        if set(edge["properties"]) != {"referenceKey"} or len(edge["replacements"]) != 1:
            raise ValueError("Unexpected SITE_OF properties or replacement count")
        replacement = edge["replacements"][0]
        if not replacement["id"]:
            raise ValueError("SITE_OF has no existing AREA_OF replacement")
        normalized = []
        for reference in edge["properties"]["referenceKey"]:
            dataset, separator, key = reference.partition(" Key: ")
            field, colon, value = key.partition(": ")
            if not separator or not colon:
                raise ValueError("Unrecognized historical referenceKey")
            normalized.append(dataset + separator + field + " == " + value)
        refs = set(replacement["properties"].get("referenceKey", []))
        actual = {r["referenceKey"] for r in support
                  if (r["site"], r["target"]) == (edge["source"], edge["target"])}
        if set(normalized) != refs or refs != actual:
            raise ValueError("SITE_OF provenance does not match canonical AREA_OF and live USES")


def recommendation(database, row):
    if database == "sociomap" and row["property"] == "language":
        return "Review moving the place CMID into district on a language source assertion; preserve dataset/key provenance and avoid existing reverse assertions."
    if database == "sociomap":
        return "Review source and referenced CMID; correct or remove the religion value. Do not automatically reverse."
    if row["property"] == "period":
        return "Review site temporal association: approve an explicit SITE exception or represent geographic context on the period. Do not automatically reverse."
    if row["targetDomains"] == ["AREA"]:
        return "Review geographic context on the culture instead of culture on the AREA assertion."
    return "Review cultural attribution of this period; under strict ordering represent period on culture, preserving source provenance."


def run(database, driver, output, apply):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            labels = tx.run(LABELS).data()
            properties = tx.run(PROPERTIES).data()
            sites = tx.run(SITE_EDGES).data() if database == "archamap" else []
            support = tx.run(SUPPORT).data() if sites else []
            verify_site_provenance(sites, support)
            desired = desired_labels(labels, database)
            policy = parsed_policy(desired, properties)
            changes = [new for old, new in zip(labels, desired) if old != new]
            summary = {"database": database, "labelUpdates": len(changes), "siteEdgesConsolidated": len(sites)}
            snapshot = {"labels": labels, "properties": properties, "sites": sites, "support": support}
            snapshot_path = output / f"{database}-{'before' if apply else 'preview'}.json"
            with snapshot_path.open('x' if apply else 'w') as stream:
                json.dump(snapshot, stream, indent=2)
            if apply:
                for new in changes:
                    result = tx.run("MATCH (n:LABEL) WHERE elementId(n)=$id SET n=$properties RETURN elementId(n) AS id", **new).single()
                    if result is None:
                        raise ValueError("Metadata node disappeared")
                existing_names = {r["properties"]["CMName"] for r in properties}
                next_id = max(int(r["properties"]["CMID"][2:]) for r in properties) + 1
                for name, metatype in [("contextualTargetMode", "string"), ("contextualTargetDomains", "list"), ("contextualOrder", "string")]:
                    if name not in existing_names:
                        tx.run("""CREATE (:METADATA:PROPERTY {CMID:$cmid,CMName:$name,type:'node',
                            metaType:$metatype,internal:true,editable:false,nodeType:'METADATA',
                            description:'Internal contextual relationship policy metadata'})""",
                            cmid=f"CP{next_id}", name=name, metatype=metatype).consume()
                        next_id += 1
                for edge in sites:
                    tx.run("MATCH ()-[r:SITE_OF]->() WHERE elementId(r)=$id DELETE r", id=edge["id"]).consume()
                if tx.run(LABELS).data() != desired:
                    raise ValueError("Metadata read-back mismatch")
                if database == "archamap" and tx.run(SITE_EDGES).data():
                    raise ValueError("SITE_OF cleanup incomplete")
                parsed_policy(tx.run(LABELS).data(), tx.run(PROPERTIES).data())
                for edge in sites:
                    found = tx.run("MATCH ()-[r:AREA_OF]->() WHERE elementId(r)=$id RETURN properties(r) AS properties", id=edge["replacements"][0]["id"]).single()
                    if found["properties"] != edge["replacements"][0]["properties"]:
                        raise ValueError("Canonical AREA_OF changed")
                for assertion in support:
                    found = tx.run("MATCH ()-[r:USES]->() WHERE elementId(r)=$id RETURN properties(r) AS properties", id=assertion["id"]).single()
                    if found["properties"] != assertion["properties"]:
                        raise ValueError("Source assertion changed")
                tx.commit()
            else:
                tx.rollback()
    if apply:
        with driver.session() as session:
            if session.run(LABELS).data() != desired:
                raise ValueError("Independent metadata read-back failed")
            if database == "archamap" and session.run(SITE_EDGES).data():
                raise ValueError("Independent SITE_OF read-back failed")
        policy = context_policy.load_policy(driver)
    conflicts = context_policy.stored_conflicts(driver, policy=policy)
    for row in conflicts:
        row["recommendation"] = recommendation(database, row)
    (output / f"{database}-cleanup-recommendations.json").write_text(json.dumps(conflicts, indent=2))
    summary["reviewOnlyAssertions"] = len({row["usesID"] for row in conflicts})
    summary["reviewOnlyLinks"] = len({(row["reference"], row["CMID"], row["relationship"]) for row in conflicts})
    summary["recommendationsTruncated"] = len(conflicts) == 10000
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--local", action="store_true", help="Use this host's published Neo4j ports")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=True)
    report = {"applied": args.apply, "timestampUtc": datetime.now(timezone.utc).isoformat(), "databases": []}
    for database, port in [("sociomap", 7687), ("archamap", 17687)]:
        if args.local:
            settings = config["DB"]
            driver = GraphDatabase.driver(f"bolt://127.0.0.1:{port}", auth=(settings["user"], settings["pwd"]))
        else:
            driver = getDriver(database)
        try:
            report["databases"].append(run(database, driver, args.output, args.apply))
        finally:
            driver.close()
    (args.output / "summary.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
