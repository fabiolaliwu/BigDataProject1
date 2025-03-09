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

    # Load nodes into MongoDB
    def loadNodes(self, node_file='nodes.tsv'):
        nodes_df = pd.read_csv(node_file, sep='\t')
        nodes_data = nodes_df.to_dict(orient='records')

        compoundNode = []
        diseaseNode = []
        anatomyNode = []
        geneNode = []
        for node in nodes_data:
            node_type = node.get('kind')
            if node_type == 'Compound':
                compoundNode.append(node)
            elif node_type == 'Disease':
                diseaseNode.append(node)
            elif node_type == 'Anatomy':
                anatomyNode.append(node)
            elif node_type == 'Gene':
                geneNode.append(node)
        if compoundNode:
            print(f"Inserting {len(compoundNode)} compound nodes into MongoDB")
            self.nodes_collection.insert_many(compoundNode)

        if diseaseNode:
            self.nodes_collection.insert_many(diseaseNode)
        if anatomyNode:
            self.nodes_collection.insert_many(anatomyNode)
        if geneNode:
            self.nodes_collection.insert_many(geneNode)
        print(f"{len(nodes_data)} nodes loaded into MongoDB")

    

