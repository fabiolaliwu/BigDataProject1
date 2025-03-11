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
        

