from pymongo import MongoClient
from neo4j import GraphDatabase
import pandas as pd

# MongoDB Connection Setup
def connect_mongo():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['hetio_database']
    return db

# Neo4j Connection Setup
def connect_neo4j():
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "fabi1020"))
    return driver


