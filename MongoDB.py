from pymongo import MongoClient
import pandas as pd

class MongoDB:
    def __init__(self, uri="mongodb://localhost:27017/", db="hetio_database"):
        self.client = MongoClient(uri)
        self.db = self.client[db]
        self.nodes_collection = self.db['nodes']
        self.edges_collection = self.db['edges']

    def cleanDatabase(self):
        self.db.nodes.drop()
        self.db.edges.drop()
        print("Database cleaned in MongoDB")
  
    def loadNodes(self, node_file='nodes.tsv'):
        nodes_df = pd.read_csv(node_file, sep='\t')
        nodes_data = nodes_df.to_dict(orient='records')
        print(f"{len(nodes_data)} nodes loaded into MongoDB") # testing purposes

    

