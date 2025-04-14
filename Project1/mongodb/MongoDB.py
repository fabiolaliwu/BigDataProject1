from pymongo import MongoClient, ASCENDING
import pandas as pd

class MongoDB:
    def __init__(self, uri="mongodb+srv://fabiolaliwu:fabiolaliwu@cluster0.nps6h.mongodb.net/", db="hetio_database"):
        self.client = MongoClient(uri)
        self.db = self.client[db]
        self.nodes_collection = self.db['nodes']
        self.edges_collection = self.db['edges']
        
        #create indexes for faster queries
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

    def diseaseInfo(self, diseaseID):
        #first get the disease name
        disease = self.nodes_collection.find_one({"_id": diseaseID})
        if not disease:
            return None
        
        disease_name = disease["name"]
        
        #query for treating drugs (CtD)
        treating_drugs_cursor = self.edges_collection.find({"metaedge": "CtD", "target": diseaseID})
        treating_drug_ids = [drug['source'] for drug in treating_drugs_cursor]
        
        #query for genes (DaG) - checking both directions 
        genes_cursor1 = self.edges_collection.find({"metaedge": "DaG", "target": diseaseID})
        genes_cursor2 = self.edges_collection.find({"metaedge": "DaG", "source": diseaseID})
        
        #combine both gene queries
        gene_ids1 = [gene['source'] for gene in genes_cursor1]  #if gene→disease
        gene_ids2 = [gene['target'] for gene in genes_cursor2]  #if disease→gene
        gene_ids = gene_ids1 + gene_ids2
        
        #query for anatomical locations (DlA)
        anatomy_cursor = self.edges_collection.find({"metaedge": "DlA", "source": diseaseID})
        anatomy_ids = [anatomy['target'] for anatomy in anatomy_cursor]
        
        #look up names in the nodes collection
        treating_drug_nodes = list(self.nodes_collection.find({"_id": {"$in": treating_drug_ids}}))
        gene_nodes = list(self.nodes_collection.find({"_id": {"$in": gene_ids}}))
        anatomy_nodes = list(self.nodes_collection.find({"_id": {"$in": anatomy_ids}}))
        
        #extract names
        treating_drug_names = [drug['name'] for drug in treating_drug_nodes]
        gene_names = [gene['name'] for gene in gene_nodes]
        anatomy_names = [anatomy['name'] for anatomy in anatomy_nodes]
        
        #return result dictionary for GUI use
        return {
            "Disease": disease_name,
            "Drugs": treating_drug_names,
            "Genes": gene_names,
            "Locations": anatomy_names
        }

    def missingEdges(self):
        """Query 2: Find potential new drug-disease relationships"""
        #step 1 -- collect all necessary edges
        cug_edges = list(self.edges_collection.find({"metaedge": "CuG"}, {"source": 1, "target": 1}))
        cdg_edges = list(self.edges_collection.find({"metaedge": "CdG"}, {"source": 1, "target": 1}))
        adg_edges = list(self.edges_collection.find({"metaedge": "AdG"}, {"source": 1, "target": 1}))
        aug_edges = list(self.edges_collection.find({"metaedge": "AuG"}, {"source": 1, "target": 1}))
        dla_edges = list(self.edges_collection.find({"metaedge": "DlA"}, {"source": 1, "target": 1}))
        ctd_edges = list(self.edges_collection.find({"metaedge": "CtD"}, {"source": 1, "target": 1}))
        
        #step 2 -- lookup dictionaries for faster access
        gene_to_cug = {}  #gene -> compounds that up regulate it
        gene_to_cdg = {}  #gene -> compounds that down regulate it
        gene_to_adg = {}  #gene -> anatomies that down regulate it
        gene_to_aug = {}  #gene -> anatomies that up regulate it
        anatomy_to_dla = {}  #anatomy -> diseases that occur there
        
        #fill lookup dictionaries
        for edge in cug_edges:
            if edge["target"] not in gene_to_cug:
                gene_to_cug[edge["target"]] = []
            gene_to_cug[edge["target"]].append(edge["source"])
            
        for edge in cdg_edges:
            if edge["target"] not in gene_to_cdg:
                gene_to_cdg[edge["target"]] = []
            gene_to_cdg[edge["target"]].append(edge["source"])
            
        for edge in adg_edges:
            if edge["target"] not in gene_to_adg:
                gene_to_adg[edge["target"]] = []
            gene_to_adg[edge["target"]].append(edge["source"])
            
        for edge in aug_edges:
            if edge["target"] not in gene_to_aug:
                gene_to_aug[edge["target"]] = []
            gene_to_aug[edge["target"]].append(edge["source"])
            
        for edge in dla_edges:
            if edge["target"] not in anatomy_to_dla:
                anatomy_to_dla[edge["target"]] = []
            anatomy_to_dla[edge["target"]].append(edge["source"])
            
        #step 3 -- set of existing drug-disease pairs to exclude
        existing_pairs = set((edge["source"], edge["target"]) for edge in ctd_edges)
        
        #step 4 -- find potential new edges
        potential_edges = set()
        
        #case 1 -- drug up-regulates gene, anatomy down-regulates gene
        for gene in gene_to_cug:
            if gene in gene_to_adg:
                for compound in gene_to_cug[gene]:
                    for anatomy in gene_to_adg[gene]:
                        if anatomy in anatomy_to_dla:
                            for disease in anatomy_to_dla[anatomy]:
                                if (compound, disease) not in existing_pairs:
                                    potential_edges.add((compound, disease))
        
        #case 2 -- drug down-regulates gene, anatomy up-regulates gene
        for gene in gene_to_cdg:
            if gene in gene_to_aug:
                for compound in gene_to_cdg[gene]:
                    for anatomy in gene_to_aug[gene]:
                        if anatomy in anatomy_to_dla:
                            for disease in anatomy_to_dla[anatomy]:
                                if (compound, disease) not in existing_pairs:
                                    potential_edges.add((compound, disease))
        
        #step 5 -- get compound names
        compound_ids = set(pair[0] for pair in potential_edges)
        
        compound_map = {}
        for compound in self.nodes_collection.find({"_id": {"$in": list(compound_ids)}}):
            compound_map[compound["_id"]] = compound["name"]
        
        #step 6 -- unique list of compound names
        unique_compounds = []
        seen = set()
        
        for compound_id in [pair[0] for pair in potential_edges]:
            if compound_id in compound_map and compound_id not in seen:
                seen.add(compound_id)
                unique_compounds.append(compound_map[compound_id])
        
        #sort compounds alphabetically
        unique_compounds.sort()
        
        #return as list of dictionaries
        return [{"compound_name": name} for name in unique_compounds]