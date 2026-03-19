# extract_graph.py
from neo4j import GraphDatabase
import json
import os

# Configure Neo4j from environment variables to avoid hardcoded credentials.
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

if not NEO4J_URI or not NEO4J_USER or not NEO4J_PASSWORD:
    raise RuntimeError("Missing Neo4j credentials. Set NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD.")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def export_graph(node_file="nodes.jsonl", rel_file="relationships.jsonl"):
    with driver.session() as session:
        # dump nodes
        with open(node_file, "w", encoding="utf-8") as nf:
            result = session.run("MATCH (n) RETURN n.CMID AS cmid, n.CMName AS CMName, labels(n) AS labels, properties(n) AS props")
            for record in result:
                obj = {
                    "type": "node",
                    "CMID": record["cmid"],
                    "CMName": record["CMName"],
                    "labels": record["labels"],
                    "properties": record["props"]
                }
                nf.write(json.dumps(obj) + "\n")

        # dump relationships
        with open(rel_file, "w", encoding="utf-8") as rf:
            result = session.run(
                "MATCH (a)-[r]->(b) RETURN type(r) AS type, "
                "elementId(a) AS start_id, elementId(b) AS end_id, properties(r) AS props"
            )
            for record in result:
                obj = {
                    "type": "relationship",
                    "rel_type": record["type"],
                    "start_id": record["start_id"],
                    "end_id": record["end_id"],
                    "properties": record["props"]
                }
                rf.write(json.dumps(obj) + "\n")

    print(f"Export complete: {node_file} & {rel_file}")

if __name__ == "__main__":
    export_graph()
