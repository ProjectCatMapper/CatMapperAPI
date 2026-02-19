# extract_graph.py
from neo4j import GraphDatabase
import json

# configure your Neo4j connection
NEO4J_URI = "neo4j://sociomap.rc.asu.edu:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "catnapperproject"

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
