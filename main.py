from pymongo import MongoClient
from neo4j import GraphDatabase
import pandas as pd

# MongoDB Connection Setup
client = MongoClient('mongodb://localhost:27017/')
db = client['hetio_database']
nodes_collection = db['nodes']


# Neo4j Connection Setup
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "fabi1020"))

# clean database before inserting nodes
def cleanDataBase():
    db.nodes.drop()
    print("Database cleaned")

# Load nodes into MongoDB
def loadNodes():
    nodes_df = pd.read_csv('nodes.tsv', sep='\t')
    nodes_data = nodes_df.to_dict(orient='records')
    nodes_collection.insert_many(nodes_data)
    print(f"{len(nodes_data)} nodes in MongoDB")

if __name__ == "__main__":
    print("Loading data into MongoDB and Neo4j...")
    cleanDataBase()
    loadNodes()
    node_count = nodes_collection.count_documents({})
    print(f"Total nodes in MongoDB: {node_count}")

