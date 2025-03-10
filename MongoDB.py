from pymongo import MongoClient
import pandas as pd

class MongoDB:
    def __init__(self, uri="mongodb+srv://fabiolaliwu:fabiolaliwu@cluster0.nps6h.mongodb.net/", db="hetio_database"):
        self.client = MongoClient(uri)
        self.db = self.client[db]
        self.nodes_collection = self.db['nodes']
        self.edges_collection = self.db['edges']

    def cleanDatabase(self):
        self.db.nodes.drop()
        self.db.edges.drop()
        print("database cleaned")

    def loadNodes(self, nodeFile='nodes.tsv'):
        nodes_df = pd.read_csv(nodeFile, sep='\t')
        nodes_data = nodes_df.to_dict(orient='records')
        compoundList = []
        diseaseList = []
        anatomyList = []
        geneList = []
        for node in nodes_data:
            if node['kind'] == 'Compound':
                compoundList.append(node)
            elif node['kind'] == 'Disease':
                diseaseList.append(node)
            elif node['kind'] == 'Anatomy':
                anatomyList.append(node)
            elif node['kind'] == 'Gene':
                geneList.append(node)
        self.nodes_collection.insert_many(compoundList)
        self.nodes_collection.insert_many(diseaseList)
        self.nodes_collection.insert_many(anatomyList)
        self.nodes_collection.insert_many(geneList)

        #testing purposes
        print(f"{len(compoundList)} inserted into the DB for compound")
        print(f"{len(diseaseList)} inserted into the DB for disease")
        print(f"{len(anatomyList)} inserted into the DB for anatomy")
        print(f"{len(geneList)} inserted into the DB for gene")

        # if nodes_data:
        #     self.nodes_collection.insert_many(nodes_data)
        #     # testing purposes
        #     print(f"{len(nodes_data)} nodes loaded into MongoDB")
        # else:
        #     print("No data to insert.")

    def loadEdges(self, edgeFile = 'edges.tsv'):
        edges_df = pd.read_csv(edgeFile, sep='\t')
        edges_data = edges_df.to_dict(orient='records')

        if edges_data:
            self.edges_collection.insert_many(edges_data)
            # testing purposes
            print(f"{len(edges_data)} nodes loaded into MongoDB")
        else:
            print("No data to insert.")
