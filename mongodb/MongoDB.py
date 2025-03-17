from pymongo import MongoClient, ASCENDING
import pandas as pd

class MongoDB:
    def __init__(self, uri="mongodb+srv://fabiolaliwu:fabiolaliwu@cluster0.nps6h.mongodb.net/", db="hetio_database"):
        self.client = MongoClient(uri)
        self.db = self.client[db]
        self.nodes_collection = self.db['nodes']
        self.edges_collection = self.db['edges']
        self.edges_collection.create_index([("source", ASCENDING)])
        self.edges_collection.create_index([("target", ASCENDING)])
        self.edges_collection.create_index([("metaedge", ASCENDING)])
        self.edges_collection.create_index([("source", ASCENDING), ("target", ASCENDING)])
        self.edges_collection.create_index([("metaedge", ASCENDING), ("source", ASCENDING)])

    def cleanDatabase(self):
        self.nodes_collection.drop()
        self.edges_collection.drop()
        print("Database cleaned")
    
    def loadNodes(self, nodeFile= "/Users/fabiolaliwu/Desktop/BigDataProject1/data/nodes.tsv"):
        nodes_df = pd.read_csv(nodeFile, sep='\t')
        nodes_df.rename(columns={'id': '_id'}, inplace=True)
        nodes_df = nodes_df[['_id', 'name', 'kind']]
        nodes_data = nodes_df.to_dict(orient='records')
        self.nodes_collection.insert_many(nodes_data)
        print(f"{len(nodes_data)} nodes uploaded into MongoDB")
    
    def loadEdges(self, edgeFile="/Users/fabiolaliwu/Desktop/BigDataProject1/data/edges.tsv"):
        edges_df = pd.read_csv(edgeFile, sep='\t')
        edges_df = edges_df[['source', 'metaedge', 'target']]
        edges_data = edges_df.to_dict(orient='records')
        self.edges_collection.insert_many(edges_data)
        print(f"{len(edges_data)} edges loaded into MongoDB")

    def get_node_names(self, ids):
        """ Helper function to fetch the names from nodes collection based on ids """
        names = []
        for node_id in ids:
            node = self.nodes_collection.find_one({"_id": node_id})
            if node and "name" in node:
                names.append(node["name"])
        return names

    def diseaseInfo(self, diseaseID):
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"metaedge": "CtD", "target": diseaseID},
                        {"metaedge": "CpD", "target": diseaseID},
                        {"metaedge": "DdG", "source": diseaseID},
                        {"metaedge": "DlA", "source": diseaseID}
                    ]
                }
            },
            {
                "$lookup": {
                    "from": "nodes",  # The collection we're joining with
                    "localField": "target",  # The field in the edges collection
                    "foreignField": "_id",  # The field in the nodes collection
                    "as": "target_node"  # The alias for the joined data
                }
            },
            {
                "$lookup": {
                    "from": "nodes",  # The collection we're joining with
                    "localField": "source",  # The field in the edges collection
                    "foreignField": "_id",  # The field in the nodes collection
                    "as": "source_node"  # The alias for the joined data
                }
            },
            {
                "$project": {
                    "source_name": {"$arrayElemAt": ["$source_node.name", 0]}, 
                    "metaedge": 1,
                    "target_name": {"$arrayElemAt": ["$target_node.name", 0]} 
                }
            }
        ]
        result = self.edges_collection.aggregate(pipeline)

        treating_drug_sources = []
        palliating_drug_sources = []
        genes_target = []
        anatomy_target = []
        for item in result:
            if item['metaedge'] == 'CtD' and item.get('source_name'):
                treating_drug_sources.append(item['source_name'])
            elif item['metaedge'] == 'CpD' and item.get('source_name'):
                palliating_drug_sources.append(item['source_name'])
            elif item['metaedge'] == 'DdG' and item.get('target_name'):
                genes_target.append(item['target_name'])
            elif item['metaedge'] == 'DlA' and item.get('target_name'):
                anatomy_target.append(item['target_name'])

        disease = self.nodes_collection.find_one({"_id": diseaseID})
        disease_name = disease["name"]

        # Print the results
        print("=====Info=====")
        print(f"ID: {diseaseID}")
        print(f"Name: {disease_name}")
        print(f"Treating Drugs: {', '.join(treating_drug_sources) if treating_drug_sources else 'No treating drugs available.'}")
        print(f"Palliating Drugs: {', '.join(palliating_drug_sources) if palliating_drug_sources else 'No palliating drugs available.'}")
        print(f"Genes: {', '.join(genes_target) if genes_target else 'No genes available.'}")
        print(f"Anatomy: {', '.join(anatomy_target) if anatomy_target else 'No anatomy available.'}")

        # info = [
        #     {"$match": {"_id": diseaseID}}, 
        #     {
        #         "$lookup": {
        #             "from": "edges",
        #             "localField": "_id",
        #             "foreignField": "target",
        #             "as": "foundEdge"
        #         }
        #     },
        #     {
        #         "$unwind": {
        #             "path": "$foundEdge",
        #             "preserveNullAndEmptyArrays": True
        #         }
        #     },
        #     {
        #         "$lookup": {
        #             "from": "nodes",
        #             "localField": "foundEdge.source",
        #             "foreignField": "_id",
        #             "as": "related_nodes"
        #         }
        #     },
        #     {
        #         "$group": {
        #             "_id": "$_id",
        #             "Name": {"$first": "$name"},
        #             "Treating drugs": {
        #                 "$addToSet": {
        #                     "$cond": [
        #                         {"$in": ["$foundEdge.metaedge", ["CtD"]]},
        #                         "$related_nodes.name",
        #                         None
        #                     ]
        #                 }
        #             },
        #             "Palliating drugs": {
        #                 "$push": {
        #                     "$cond": [
        #                         {"$in": ["$foundEdge.metaedge", ["CpD"]]},
        #                         "$related_nodes.name",
        #                         None
        #                     ]
        #                 }
        #             },
        #             "genes": {
        #                 "$push": {
        #                     "$cond": [
        #                         {"$eq": ["$foundEdge.metaedge", "DaG"]}, 
        #                         "$related_nodes.name",
        #                         None
        #                     ]
        #                 }
        #             },
        #             "anatomy": {
        #                 "$addToSet": {
        #                     "$cond": [
        #                         {"$eq": ["$foundEdge.metaedge", "DlA"]},  # Anatomy (metaedge = 'DlA')
        #                         "$related_nodes.name",
        #                         None
        #                     ]
        #                 }
        #             }
        #         }
        #     },
        #     {
        #         "$project": {
        #             "_id": 0,
        #             "ID": "$_id",
        #             "Name": 1,
        #             "Treating drugs": {"$filter": {"input": "$Treating drugs", "as": "item", "cond": {"$ne": ["$$item", None]}}},
        #             "Palliating drugs": {"$filter": {"input": "$Palliating drugs", "as": "item", "cond": {"$ne": ["$$item", None]}}},
        #             "Genes": {"$filter": {"input": "$genes", "as": "item", "cond": {"$ne": ["$$item", None]}}},
        #             "Anatomy": {"$filter": {"input": "$anatomy", "as": "item", "cond": {"$ne": ["$$item", None]}}}
        #         }
        #     }
        # ]
        # result = list(self.nodes_collection.aggregate(info))
        # if result:
        #     disease = result[0]
            
        #     # Print disease name
        #     print(f"Name: {disease['Name']}")
            
        #     # Flatten Treating Drugs list if it's a list of lists
        #     treating_drugs = [drug for sublist in disease['Treating drugs'] for drug in sublist]
        #     print(f"Treating Drugs: {', '.join(treating_drugs) if treating_drugs else 'No treating drugs available'}")

        #     # Flatten Palliating Drugs list if it's a list of lists
        #     palliating_drugs = [drug for sublist in disease['Palliating drugs'] for drug in sublist]
        #     print(f"Palliating Drugs: {', '.join(palliating_drugs) if palliating_drugs else 'No palliative drugs available'}")
            
        #     # Print Genes
        #     genes = disease.get('Genes', [])
        #     if genes:
        #         print(f"Genes: {', '.join(genes)}")
        #     else:
        #         print("Genes: No genes data available")
            
        #     # Print Anatomy
        #     anatomy = disease.get('Anatomy', [])
        #     if anatomy:
        #         print(f"Anatomy: {', '.join(anatomy)}")
        #     else:
        #         print("Anatomy: No anatomical data available")
        
        # else:
        #     print("No results found for the given disease ID.")




 # # Query for edges with the specific metaedges and diseaseID as source/target
        # treating_drugs_cursor = self.edges_collection.find({"metaedge": "CtD", "target": diseaseID})
        # palliating_drugs_cursor = self.edges_collection.find({"metaedge": "CpD", "target": diseaseID})
        # genes_cursor = self.edges_collection.find({"metaedge": "DdG", "source": diseaseID})
        # anatomy_cursor = self.edges_collection.find({"metaedge": "DlA", "source": diseaseID})

        # # Convert the cursors to lists so we can iterate multiple times
        # treating_drugs = list(treating_drugs_cursor)
        # palliating_drugs = list(palliating_drugs_cursor)
        # genes = list(genes_cursor)
        # anatomy = list(anatomy_cursor)

        # # Debug: Print the number of matching results and the results themselves
        # print(f"Number of treating drugs: {len(treating_drugs)}")
        # print(f"Number of palliating drugs: {len(palliating_drugs)}")
        # print(f"Number of genes: {len(genes)}")
        # print(f"Number of anatomy: {len(anatomy)}")

        # # Lists to store the relevant source/target values
        # treating_drug_sources = []
        # palliating_drug_sources = []
        # genes_target = []
        # anatomy_target = []

        # # Print the entire documents for genes and anatomy (for debugging)
        # print("\nGenes Documents:")
        # for drug in genes:
        #     print(drug)  # Print the entire document for debugging
            
        # print("\nAnatomy Documents:")
        # for drug in anatomy:
        #     print(drug)  # Print the entire document for debugging

        # # Iterate through the list and store the 'source' or 'target' in the list
        # for drug in treating_drugs:
        #     if 'source' in drug:  # Ensure the key exists
        #         treating_drug_sources.append(drug['source'])
        # for drug in palliating_drugs:
        #     if 'source' in drug:  # Ensure the key exists
        #         palliating_drug_sources.append(drug['source'])
        # for drug in genes:
        #     if 'target' in drug:  # Ensure the key exists
        #         genes_target.append(drug['target'])
        # for drug in anatomy:
        #     if 'target' in drug:  # Ensure the key exists
        #         anatomy_target.append(drug['target'])

        # # Print the results
        # if treating_drug_sources:
        #     print(f"Treating Drugs: {', '.join(treating_drug_sources)}")
        # else:
        #     print("No treating drugs available.")

        # if palliating_drug_sources:
        #     print(f"Palliating Drugs: {', '.join(palliating_drug_sources)}")
        # else:
        #     print("No palliating drugs available.")

        # if genes_target:
        #     print(f"Genes: {', '.join(genes_target)}")
        # else:
        #     print("No genes available.")

        # if anatomy_target:
        #     print(f"Anatomy: {', '.join(anatomy_target)}")
        # else:
        #     print("No anatomy available.")


    # def matchEdgesByTarget(self):
     
    #     # Step 1: Get all the edges once and group them by metaedge
    #     all_edges = list(self.edges_collection.find({}))  # Fetch all edges
        
    #     # Group edges by metaedge, including DlA for filtering later
    #     grouped_edges = {
    #         "AuG": [],
    #         "CdG": [],
    #         "AdG": [],
    #         "CuG": [],
    #         "DlA": []  # This will be used to filter AuG and AdG edges
    #     }

    #     # Step 2: Iterate through the edges and group them based on metaedge
    #     for edge in all_edges:
    #         metaedge = edge.get("metaedge")
    #         if metaedge in grouped_edges:
    #             grouped_edges[metaedge].append(edge)
        
    #     # Debugging: print the lengths of edges per metaedge
    #     for metaedge, edges in grouped_edges.items():
    #         print(f"Length of {metaedge} edges: {len(edges)}")
        
    #     # Step 3: Find the target of DlA in the nodes collection
    #     dlA_targets = {edge["target"] for edge in grouped_edges["DlA"]}
    #     print(f"Number of DlA targets: {len(dlA_targets)}")

    #     # Fetch all nodes in the collection
    #     all_nodes = list(self.nodes_collection.find({}))  # Fetch all nodes
    #     found_node_ids = []

    #     # Step 4: Match the DlA targets with nodes
    #     for target in dlA_targets:
    #         # Find nodes whose target matches the DlA target
    #         for node in all_nodes:
    #             if node.get("target", "").strip() == target:  # Ensure we're comparing the correct field
    #                 # Only add the node ID if it's not already in found_node_ids
    #                 if node["_id"] not in found_node_ids:
    #                     found_node_ids.append(node["_id"])  # Add the node ID if a match is found
    #                     print(f"Found node for target {target}: {node['_id']}")
    #                 break  # No need to continue checking other nodes for this target
        
    #     print(f"Found node IDs for DlA targets: {found_node_ids}")


    #     # Filter AuG and AdG edges: only keep those whose source is in the DlA target list
    #     grouped_edges["AuG"] = [edge for edge in grouped_edges["AuG"] if edge["source"] in dlA_targets]
    #     grouped_edges["AdG"] = [edge for edge in grouped_edges["AdG"] if edge["source"] in dlA_targets]

    #     print("\n after filtering oout ")

    #     for metaedge, edges in grouped_edges.items():
    #         print(f"Length of {metaedge} edges: {len(edges)}")
        
        # # Initialize a list to store the matching results
        # result = []

        # # Step 4: Compare targets between AuG and CdG edges
        # for aug_edge in grouped_edges["AuG"]:
        #     aug_target = aug_edge["target"]
        #     # Check if any CdG edge has the same target
        #     for cdg_edge in grouped_edges["CdG"]:
        #         if cdg_edge["target"] == aug_target:
        #             # If they match, store the matching pair
        #             result.append({
        #                 "AuG_edge": aug_edge,
        #                 "CdG_edge": cdg_edge
        #             })
        
        # # Step 5: Compare targets between AdG and CuG edges
        # for adg_edge in grouped_edges["AdG"]:
        #     adg_target = adg_edge["target"]
        #     # Check if any CuG edge has the same target
        #     for cug_edge in grouped_edges["CuG"]:
        #         if cug_edge["target"] == adg_target:
        #             # If they match, store the matching pair
        #             result.append({
        #                 "AdG_edge": adg_edge,
        #                 "CuG_edge": cug_edge
        #             })

        # # Step 6: Print the results
        # for match in result:
        #     if "AuG_edge" in match and "CdG_edge" in match:
        #         print(f"Matching AuG and CdG pair: AuG from {match['AuG_edge']['source']} to Gene::{match['AuG_edge']['target']}")
        #         print(f"Matching CdG from {match['CdG_edge']['source']} to Gene::{match['CdG_edge']['target']}")
        #     if "AdG_edge" in match and "CuG_edge" in match:
        #         print(f"Matching AdG and CuG pair: AdG from {match['AdG_edge']['source']} to Gene::{match['AdG_edge']['target']}")
        #         print(f"Matching CuG from {match['CuG_edge']['source']} to Gene::{match['CuG_edge']['target']}")
        #     print("-" * 40)

        # print(f"Total results found: {len(result)}")


            








    

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





    # def matchEdgesByTarget(self):
    #     # Step 1: Get all the edges with metaedge 'DlA'
    #     DlAEdges = list(self.edges_collection.find({"metaedge": "DlA"}))  # Fetch all edges with metaedge 'DlA'
        
    #     # Step 2: Debugging to check how many edges we fetched
    #     print(f"Number of edges with metaedge 'DlA': {len(DlAEdges)}")

    #     # Step 1: Get all the edges with metaedge 'DlA'
    #     AuGEdges = list(self.edges_collection.find({"metaedge": "AuG"}))  # Fetch all edges with metaedge 'DlA'
    #     print(f"Number of edges with metaedge 'AuG': {len(AuGEdges)}")

    #     # for dlaEdge in DlAEdges:
    #     #     print(f"DlA target: {dlaEdge['target']}")
    #     # for aug_edge in AuGEdges:
    #     #     print(f"AuG source: {aug_edge['source']}")
    #     AuGFiltered = list()

    #     for aug_edge in AuGEdges:
    #         for dlaEdge in DlAEdges:
    #             if dlaEdge['target'] == aug_edge['source']:
    #                 # print(f"DlA target: {dlaEdge['target']}")
    #                 # print(f"AuG source: {aug_edge['source']}")
                    
    #                 AuGFiltered.append(aug_edge)
    #                 break
                    

    #     print(f"Number of edges with metaedge 'AuG': {len(AuGEdges)}")
    #     print(f"Number of edges with filtered metaedge 'AuG': {len(AuGFiltered)}")

        
    



# time limit issue
# def findMatchingEdges(self):
#         try:
#             pipeline = [
#                 # Step 1: Match all CuG and CdG edges
#                 {
#                     "$match": {"metaedge": {"$in": ["CuG", "CdG"]}}
#                 },
#                 # Step 2: Use a single $lookup to match both AdG for CuG and AuG for CdG based on target
#                 {
#                     "$lookup": {
#                         "from": "edges",
#                         "let": {
#                             "target": "$target",
#                             "metaedge_type": "$metaedge"
#                         },
#                         "pipeline": [
#                             # Match based on target and metaedge conditions
#                             {
#                                 "$match": {
#                                     "$expr": {
#                                         "$and": [
#                                             # Ensure target matches
#                                             {"$eq": ["$target", "$$target"]},
#                                             # Conditionally match based on CuG → AdG or CdG → AuG
#                                             {"$or": [
#                                                 {"$and": [{"$eq": ["$metaedge", "AdG"]}, {"$eq": ["$$metaedge_type", "CuG"]}]},
#                                                 {"$and": [{"$eq": ["$metaedge", "AuG"]}, {"$eq": ["$$metaedge_type", "CdG"]}]}
#                                             ]}
#                                         ]
#                                     }
#                                 }
#                             },
#                             # Only project necessary fields
#                             {"$project": {"_id": 0, "source": 1, "metaedge": 1, "target": 1}}
#                         ],
#                         "as": "matching_edges"
#                     }
#                 },
#                 # Step 3: Filter out edges without matching AdG/AuG edges
#                 {
#                     "$match": {
#                         "matching_edges": {"$ne": []}
#                     }
#                 },
#                 # Step 4: Lookup for matching DlA edges for each Matched AdG/AuG edge
#                 {
#                     "$lookup": {
#                         "from": "edges",
#                         "let": {
#                             "matched_sources": "$matching_edges.source"
#                         },
#                         "pipeline": [
#                             {
#                                 "$match": {
#                                     "$expr": {
#                                         "$and": [
#                                             {"$eq": ["$metaedge", "DlA"]},
#                                             {"$in": ["$target", "$$matched_sources"]}  # Match if source of AdG/AuG is the target of DlA
#                                         ]
#                                     }
#                                 }
#                             },
#                             {"$project": {"_id": 0, "source": 1, "metaedge": 1, "target": 1}}  # Only project the necessary fields
#                         ],
#                         "as": "dlA_edges"
#                     }
#                 },
#                 # Step 5: Create the new CtD edges and project the results
#                 {
#                     "$project": {
#                         "Edge": {"source": "$source", "metaedge": "$metaedge", "target": "$target"},
#                         "Matched_AdG_AuG_edges": "$matching_edges",
#                         "DlA_edges": "$dlA_edges",
#                         "New_CtD_Edges": {
#                             "$map": {
#                                 "input": "$dlA_edges",
#                                 "as": "dlA_edge",
#                                 "in": {
#                                     "$let": {
#                                         "vars": {
#                                             "cu_or_cd_source": "$source"  # CuG/CdG source
#                                         },
#                                         "in": {
#                                             "source": "$$cu_or_cd_source",  # Source from CuG or CdG
#                                             "metaedge": "CtD",
#                                             "target": "$$dlA_edge.source"  # Target from DlA edge
#                                         }
#                                     }
#                                 }
#                             }
#                         }
#                     }
#                 }
#             ]

#             # Execute the aggregation pipeline
#             results = list(self.edges_collection.aggregate(pipeline))

#             # Step 6: Print the results
#             if results:
#                 matching_edges_count = 0
#                 for result in results:
#                     edge = result['Edge']
#                     print(f"Edge: {edge}")
                    
#                     for matched_edge in result['Matched_AdG_AuG_edges']:
#                         print(f"    Matched AdG/AuG Edge: {matched_edge}")
                    
#                     for new_ctd_edge in result['New_CtD_Edges']:
#                         print(f"    New CtD Edge: {new_ctd_edge}")
                    
#                     print("-" * 40)
#                     matching_edges_count += 1

#                 print(f"Total matching CuG/CdG edges: {matching_edges_count}")
#             else:
#                 print("No matching edges found.")

#         except Exception as e:
#             print(f"An error occurred: {e}")








    # def findMatchingEdges(self):
   
    #     pipeline = [
    #         # Step 1: Match all CuG and CdG edges
    #         {
    #             "$match": {"metaedge": {"$in": ["CuG", "CdG"]}}
    #         },
    #         # Step 2: Use a single $lookup to match both AdG for CuG and AuG for CdG based on target
    #         {
    #             "$lookup": {
    #                 "from": "edges",
    #                 "let": {
    #                     "target": "$target",
    #                     "metaedge_type": "$metaedge"
    #                 },
    #                 "pipeline": [
    #                     # Match based on target and metaedge conditions
    #                     {
    #                         "$match": {
    #                             "$expr": {
    #                                 "$and": [
    #                                     # Ensure target matches
    #                                     {"$eq": ["$target", "$$target"]},
    #                                     # Conditionally match based on CuG → AdG or CdG → AuG
    #                                     {"$or": [
    #                                         {"$and": [{"$eq": ["$metaedge", "AdG"]}, {"$eq": ["$$metaedge_type", "CuG"]}]},
    #                                         {"$and": [{"$eq": ["$metaedge", "AuG"]}, {"$eq": ["$$metaedge_type", "CdG"]}]}
    #                                     ]}
    #                                 ]
    #                             }
    #                         }
    #                     },
    #                     # Only project necessary fields
    #                     {"$project": {"_id": 0, "source": 1, "metaedge": 1, "target": 1}}
    #                 ],
    #                 "as": "matching_edges"
    #             }
    #         },
    #         # Step 3: Filter out edges without matching AdG/AuG edges
    #         {
    #             "$match": {
    #                 "matching_edges": {"$ne": []}
    #             }
    #         },
    #         # Step 4: Project the results to include CuG/CdG and their matched AdG/AuG edges
    #         {
    #             "$project": {
    #                 "Edge": {"source": "$source", "metaedge": "$metaedge", "target": "$target"},
    #                 "Matched_AdG_AuG_edges": "$matching_edges"
    #             }
    #         }
    #     ]
    #     results = list(self.edges_collection.aggregate(pipeline))

    #     # Step 5: Print the results
    #     if results:
    #         matching_edges_count = 0
    #         for result in results:
    #             edge = result['Edge']
    #             print(f"Edge: {edge}")
    #             for matched_edge in result['Matched_AdG_AuG_edges']:
    #                 print(f"    Matched AdG/AuG Edge: {matched_edge}")
    #             print("-" * 40)
    #             matching_edges_count += 1

    #         print(f"Total matching CuG/CdG edges: {matching_edges_count}")
    #     else:
    #         print("No matching edges found.")


# 


    def missingEdges(self):
        pipeline = [
            # Step 1: Match all CuG and CdG edges
            {
                "$match": {"metaedge": {"$in": ["CuG", "CdG"]}}
            },
            # Step 2: Use a single $lookup to match both AdG for CuG and AuG for CdG based on target
            {
                "$lookup": {
                    "from": "edges",
                    "let": {
                        "target": "$target",
                        "metaedge_type": "$metaedge"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$target", "$$target"]},
                                        {
                                            "$or": [
                                                {"$and": [{"$eq": ["$metaedge", "AdG"]}, {"$eq": ["$$metaedge_type", "CuG"]}]},
                                                {"$and": [{"$eq": ["$metaedge", "AuG"]}, {"$eq": ["$$metaedge_type", "CdG"]}]}
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        {"$project": {"_id": 0, "source": 1, "metaedge": 1, "target": 1}}
                    ],
                    "as": "matching_edges"
                }
            },
            # Step 3: Filter out edges without matching AdG/AuG edges
            {
                "$match": {
                    "matching_edges": {"$ne": []}
                }
            },
            # Step 4: Project the results to include CuG/CdG and their matched AdG/AuG edges
            {
                "$project": {
                    "Edge": {"source": "$source", "metaedge": "$metaedge", "target": "$target"},
                    "Matched_AdG_AuG_edges": "$matching_edges"
                }
            }
        ]
        results = list(self.edges_collection.aggregate(pipeline))
        DlAEdges = list(self.edges_collection.find({"metaedge": "DlA"}))
        #print(f"Found {len(DlAEdges)} DlA edges.")
        if results:
            CtD_edges = []
            matching_edges_count = 0
            for result in results:
                edge = result['Edge']  # CuG/CdG edge
                for matched_edge in result['Matched_AdG_AuG_edges']:
                    for dla_edge in DlAEdges:
                        if matched_edge['source'] == dla_edge['target']:  #AdG/AuG source with DlA target
                            ctD_edge = {
                                "source": edge['source'],  # CuG/CdG source
                                "metaedge": "CtD",
                                "target": dla_edge['source']  # DlA source
                            }
                            CtD_edges.append(ctD_edge)
                            #print(f"Created CtD Edge: {ctD_edge}")
                            matching_edges_count += 1
                    #print("-" * 40)
            CtDEdges = list(self.edges_collection.find({"metaedge": "CtD"}))
            existing_ctd_set = set((edge['source'], edge['target']) for edge in CtDEdges)
            counter = 0
            for ctD_edge in CtD_edges:
                if (ctD_edge['source'], ctD_edge['target']) not in existing_ctd_set:
                    print(f"{ctD_edge}")
                    counter += 1
            print(f"Total matching CtD edges: {matching_edges_count}")
            print(f"Found {len(CtDEdges)} CtD edges in the database.")
            print(f"There are {counter} missing edges.")
        else:
            print("No matching edges found.")
