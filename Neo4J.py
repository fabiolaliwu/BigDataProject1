# http://localhost:7474
import pandas as pd
from neo4j import GraphDatabase
from pymongo import MongoClient

class Neo4J:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="fabi1020"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def cleanDatabase(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Neo4j cleaned")


    # def loadNodes(self, node_file='nodes.tsv'):
    #     nodes_df = pd.read_csv(node_file, sep='\t')

    #     with self.driver.session() as session:
    #         for _, row in nodes_df.iterrows():
    #             session.run(
    #                 """
    #                 MERGE (n:Node {id: $id, name: $name, kind: $kind})
    #                 """,
    #                 id=row["id"], name=row["name"], kind=row["kind"]
    #             )
    #     print(f"{len(nodes_df)} nodes loaded into Neo4j")