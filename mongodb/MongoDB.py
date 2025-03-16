from pymongo import MongoClient, ASCENDING
import pandas as pd

class MongoDB:
    def __init__(self, uri="mongodb+srv://fabiolaliwu:fabiolaliwu@cluster0.nps6h.mongodb.net/", db="hetio_database"):
        self.client = MongoClient(uri)
        self.db = self.client[db]
        self.nodes_collection = self.db['nodes']
        self.edges_collection = self.db['edges']

        # Optimize queries with proper indexing
        self.edges_collection.create_index([("source", ASCENDING)])
        self.edges_collection.create_index([("target", ASCENDING)])
        self.edges_collection.create_index([("metaedge", ASCENDING)])
        self.edges_collection.create_index([("source", ASCENDING), ("target", ASCENDING)])
        
        # Optional: You can create additional compound or partial indexes here, if needed
        # For example, if you know you will frequently query on metaedge with source and target
        self.edges_collection.create_index([("metaedge", ASCENDING), ("source", ASCENDING)])

    def cleanDatabase(self):
        self.nodes_collection.drop()
        self.edges_collection.drop()
        print("Database cleaned")
    
    def loadNodes(self, nodeFile='nodes.tsv'):
        nodes_df = pd.read_csv(nodeFile, sep='\t')
        nodes_df.rename(columns={'id': '_id'}, inplace=True)
        nodes_data = nodes_df.to_dict(orient='records')
        self.nodes_collection.insert_many(nodes_data)
        print(f"{len(nodes_data)} nodes loaded into MongoDB")
    
    def loadEdges(self, edgeFile='edges.tsv'):
        edges_df = pd.read_csv(edgeFile, sep='\t')
        edges_data = edges_df.to_dict(orient='records')
        self.edges_collection.insert_many(edges_data)
        print(f"{len(edges_data)} edges loaded into MongoDB")

    def diseaseInfo(self, diseaseID):
        info = [
            {"$match": {"_id": diseaseID}}, 
            {
                "$lookup": {
                    "from": "edges",
                    "localField": "_id",
                    "foreignField": "target",
                    "as": "foundEdge"
                }
            },
            {
                "$unwind": {
                    "path": "$foundEdge",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "nodes",
                    "localField": "foundEdge.source",
                    "foreignField": "_id",
                    "as": "related_nodes"
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "Name": {"$first": "$name"},
                    "Treating drugs": {
                        "$addToSet": {
                            "$cond": [
                                {"$in": ["$foundEdge.metaedge", ["CtD"]]},
                                "$related_nodes.name",
                                None
                            ]
                        }
                    },
                    "Palliating drugs": {
                        "$push": {
                            "$cond": [
                                {"$in": ["$foundEdge.metaedge", ["CpD"]]},
                                "$related_nodes.name",
                                None
                            ]
                        }
                    },
                    "genes": {
                        "$addToSet": {
                            "$cond": [
                                {"$eq": ["$foundEdge.metaedge", "DdG"]},
                                "$related_nodes.name",
                                None
                            ]
                        }
                    },
                    "anatomy": {
                        "$addToSet": {
                            "$cond": [
                                {"$eq": ["$foundEdge.metaedge", "DlA"]},
                                "$related_nodes.name",
                                None
                            ]
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "ID": "$_id",
                    "Name": 1,
                    "Treating drugs": {"$filter": {"input": "$Treating drugs", "as": "item", "cond": {"$ne": ["$$item", None]}}},
                    "Palliating drugs": {"$filter": {"input": "$Palliating drugs", "as": "item", "cond": {"$ne": ["$$item", None]}}},
                    "Genes": {"$filter": {"input": "$genes", "as": "item", "cond": {"$ne": ["$$item", None]}}},
                    "Anatomy": {"$filter": {"input": "$anatomy", "as": "item", "cond": {"$ne": ["$$item", None]}}}
                }
            }
        ]
        result = list(self.nodes_collection.aggregate(info))
        if result:
            disease = result[0]
            print(f"Name: {disease['Name']}")
            print(f"Treating Drugs: {disease['Treating drugs']}")
            palliating_drugs = [drug for sublist in disease['Palliating drugs'] for drug in sublist]
            print(f"Palliating Drugs: {', '.join(palliating_drugs) if palliating_drugs else ' '}")
            print(f"Genes: {disease['Genes']}")
            print(f"Anatomy: {disease['Anatomy']}")

    def findMatchingEdges(self):
        pipeline = [
            # Step 1: Match all CuG edges
            {
                "$match": {"metaedge": "CuG"}
            },

            # Step 2: Lookup for matching AdG edges with the same target
            {
                "$lookup": {
                    "from": "edges",
                    "let": {"cu_target": "$target"},
                    "pipeline": [
                        {"$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$metaedge", "AdG"]},
                                    {"$eq": ["$target", "$$cu_target"]}
                                ]
                            }
                        }},
                        {"$project": {"_id": 0, "source": 1, "metaedge": 1, "target": 1}}  # Select relevant fields
                    ],
                    "as": "adg_edges"
                }
            },

            # Step 3: Match only CuG edges that have corresponding AdG edges
            {
                "$match": {
                    "adg_edges": {"$ne": []}  # Ensure there is at least one matching AdG edge
                }
            },

            # Step 4: Project the results to include both CuG and AdG edges
            {
                "$project": {
                    "CuG_edge": {"source": "$source", "metaedge": "$metaedge", "target": "$target"},
                    "AdG_edges": "$adg_edges"
                }
            }
        ]

        # Execute the aggregation pipeline
        results = list(self.edges_collection.aggregate(pipeline))

        # Step 5: Print the results
        for result in results:
            cu_g_edge = result['CuG_edge']
            for ad_g_edge in result['AdG_edges']:
                print(f"CuG Edge: {cu_g_edge}")
                print(f"AdG Edge: {ad_g_edge}")
                print("-" * 40)

        print(len(results))
    
   






   

    # def findMissingEdges(self):
    #     pipeline = [
    #         # Match edges with metaedge: 'DlA'
    #         {"$match": {"metaedge": "DlA"}},

    #         # Lookup AdG edges where the source matches the DlA target
    #         {
    #             "$lookup": {
    #                 "from": "edges",
    #                 "let": {"target_value": "$target"},  # Let the target from DlA be used
    #                 "pipeline": [
    #                     # Match edges where metaedge is 'AdG' and source is the same as DlA target
    #                     {"$match": {"$expr": {
    #                         "$and": [
    #                             {"$eq": ["$metaedge", "AdG"]},
    #                             {"$eq": ["$source", "$$target_value"]}  # Match source of AdG with target of DlA
    #                         ]
    #                     }}},
    #                     {"$project": {"_id": 0, "source": 1, "metaedge": 1, "target": 1}}  # Only include relevant fields
    #                 ],
    #                 "as": "matching_adg_edges"  # Alias for the found AdG edges
    #             }
    #         },
    #     ]

    #     # Execute the aggregation pipeline
    #     result = list(self.edges_collection.aggregate(pipeline))

    #     # Print the results
    #     if result:
    #         print(f"Found {len(result)} DlA edges with matching AdG edges:")
    #         for edge in result:
    #             # Print DlA edge
    #             print(f"DlA Edge: Source: {edge['source']} → Target: {edge['target']}")
    #             print(f"Looking for matching AdG edges where AdG Source = {edge['target']}")

    #             # Print matching AdG edges
    #             if edge['matching_adg_edges']:
    #                 for adg_edge in edge['matching_adg_edges']:
    #                     print(f"  AdG Edge: Source: {adg_edge['source']} → Target: {adg_edge['target']}")
    #             else:
    #                 print(f"  No matching AdG edges found for DlA Edge: Source: {edge['source']} → Target: {edge['target']}")
    #     else:
    #         print("No DlA edges found with matching AdG edges.")
