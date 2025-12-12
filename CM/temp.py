from neo4j import GraphDatabase

class Neo4jWriter:
    """
    Handles writes for:
        - Creating STACK nodes
        - Linking MERGING ↔ STACK
        - Linking STACK ↔ DATASET
    Uses MERGE so duplicates aren't created.
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def insert_stack(self, stackID):
        cypher = """
        MERGE (s:STACK {CMID: $stackID})
        RETURN s
        """
        with self.driver.session() as session:
            session.run(cypher, stackID=stackID)


    def insert_merging_stack(self, mergingID, stackID):
        cypher = """
        MATCH (m:MERGING {CMID: $mergingID})
        MATCH (s:STACK {CMID: $stackID})
        MERGE (m)-[:MERGING]->(s)
        """
        with self.driver.session() as session:
            session.run(cypher, mergingID=mergingID, stackID=stackID)

   
    def insert_stack_dataset(self, stackID, datasetID):
        cypher = """
        MATCH (s:STACK {CMID: $stackID})
        MATCH (d:DATASET {CMID: $datasetID})
        MERGE (s)-[:CONTAINS]->(d)
        """
        with self.driver.session() as session:
            session.run(cypher, stackID=stackID, datasetID=datasetID)


    def close(self):
        self.driver.close()
