from neo4j import GraphDatabase
import pandas as pd

class Neo4J:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="fabi1020"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.session = self.driver.session()

    def cleanDatabase(self):
        cypher_query = """
        MATCH (n)
        DETACH DELETE n
        """
        self.session.run(cypher_query)

    # from MongoDB to Neo4j
    def importNodes(self, nodes_collection):
        #finds node
        compound_nodes = list(nodes_collection.find({"kind": "Compound"}))
        disease_nodes = list(nodes_collection.find({"kind": "Disease"}))
        anatomy_nodes = list(nodes_collection.find({"kind": "Anatomy"}))
        gene_nodes = list(nodes_collection.find({"kind": "Gene"}))
    
        #load nodes into Neo4J
        if compound_nodes:
            for node in compound_nodes:
                cypher_query = f"""
                MERGE (n:Compound {{id: '{node['_id']}', name: '{node['name']}', kind: '{node['kind']}'}})
                """
        if disease_nodes:
            for node in disease_nodes:
                cypher_query = f"""
                MERGE (n:Disease {{id: '{node['_id']}', name: '{node['name']}', kind: '{node['kind']}'}})
                """
        if anatomy_nodes:
            for node in anatomy_nodes:
                cypher_query = f"""
                MERGE (n:Anatomy {{id: '{node['_id']}', name: '{node['name']}', kind: '{node['kind']}'}})
                """
        if gene_nodes:
            for node in gene_nodes:
                cypher_query = f"""
                MERGE (n:Gene {{id: '{node['_id']}', name: '{node['name']}', kind: '{node['kind']}'}})
                """
 
    def close(self):
        self.session.close()
        print("Neo4j session closed")
