from neo4j import GraphDatabase
import pandas as pd

class Neo4J:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="fabi1020"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def cleanDatabase(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Neo4j database cleaned.")
    def escape_apostrophes(self, name):
        return name.replace("'", "\\'")


    def importNodes(self, nodes_collection):
        with self.driver.session() as session:
            for node in nodes_collection.find():
                node_id = node['_id']
                name = self.escape_apostrophes(node['name'])  # Escape apostrophes
                kind = node['kind']

                cypher_query = f"""
                MERGE (n:{kind} {{id: '{node_id}', name: '{name}'}})
                """
                try:
                    session.run(cypher_query)
                except Exception as e:
                    print(f"Error inserting {kind} node {node_id}: {e}")

    def loadEdges(self, edge_file='edges.tsv'):
        edges_df = pd.read_csv(edge_file, sep='\t')
        with self.driver.session() as session:
            for _, row in edges_df.iterrows():
                source = row['source']
                metaedge = row['metaedge']
                target = row['target']

                cypher_query = f"""
                MATCH (source {{id: '{source}'}}), (target {{id: '{target}'}})
                MERGE (source)-[:{metaedge}]->(target)
                """
                try:
                    session.run(cypher_query)
                except Exception as e:
                    print(f"Error creating edge {source} -> {target}: {e}")

        print("Edges successfully loaded into Neo4j.")

    def close(self):
        self.driver.close()
        print("Neo4j connection closed.")
