from pymongo import MongoClient
from neo4j import GraphDatabase
import pandas as pd

# MongoDB Connection Setup
client = MongoClient('mongodb://localhost:27017/')
db = client['hetio_database']


# Neo4j Connection Setup
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "fabi1020"))



