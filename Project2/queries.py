import pandas as pd

class Queries:
    def __init__(self):
        self.nodes = pd.read_csv('data/nodes.tsv', sep='\t')
        #print("Node columns:", self.nodes.columns.tolist())
        # print("Node rows:", self.nodes.shape[0])
        self.edges = pd.read_csv('data/edges.tsv', sep='\t')
        # print("Node rows:", self.edges.shape[0])
        
 
    def query1(self):
        # filter all the compounds
        compounds = self.edges[self.edges['source'].str.contains('Compound')]
        # print("Compounds:", compounds)
        genesAssociated = compounds[compounds['target'].str.contains('Gene')]
        # print("Genes Associated:", genesAssociated)
        diseaseAssociated = compounds[compounds['target'].str.contains('Disease')]
        # print(diseaseAssociated)
        # # map the <compound, count> pairs to a dictionary
        genesCount = {}
        for x in genesAssociated['source']:
            if x in genesCount:
                genesCount[x] += 1
            else:
                genesCount[x] = 1
        sortedGenesCount = sorted(genesCount.items(), key=lambda x: x[1], reverse=True)

        diseaseCount = {}
        for x in diseaseAssociated['source']:
            if x in diseaseCount:
                diseaseCount[x] += 1
            else:
                diseaseCount[x] = 1

        print(f"\n{'Compound ID':<20}{'Genes':<10}{'Diseases':<10}")
        print("-" * 40)
        for compound_id, gene_count in sortedGenesCount[:5]:
            disease_count = diseaseCount.get(compound_id, 0)
            print(f"{compound_id:<20}{gene_count:<10}{disease_count:<10}")
    
    def query2(self, n):
        # filter all the compounds
        compounds = self.edges[self.edges['source'].str.contains('Compound')]
