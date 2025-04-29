import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from pyspark.sql.functions import desc

class Queries:
    def __init__(self):
        self.nodes = pd.read_csv('data/nodes.tsv', sep='\t')
        self.edges = pd.read_csv('data/edges.tsv', sep='\t')   
 
    def query(self, queryNumber):
        # filter all the compounds
        compounds = self.edges[self.edges['source'].str.contains('Compound')]
        genesAssociated = compounds[compounds['target'].str.contains('Gene')]
        diseaseAssociated = compounds[compounds['target'].str.contains('Disease')]
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
        if queryNumber == 1:
            print(f"\n\n\n\n\nQuery 1: Top 5 compounds with most genes associated, and their associated diseases")
            print(f"{'Compound ID':<20}{'Genes':<10}{'Diseases':<10}")
            print("-" * 40)
            for compound_id, gene_count in sortedGenesCount[:5]:
                disease_count = diseaseCount.get(compound_id, 0)
                print(f"{compound_id:<20}{gene_count:<10}{disease_count:<10}")
        elif queryNumber == 3:
            print(f"\n\n\n\n\nQuery 3: Names of top 5 compounds with most genes associated")
            print("-" * 60)
            for compound_id, gene_count in sortedGenesCount[:5]:
                # Get the name of the compound from the nodes dataframe
                print(f"{self.nodes[self.nodes['id'] == compound_id]['name'].values[0]} -> {gene_count}")

    def query2(self, n):
        # filter all the compounds
        drugsWithDisease = self.edges[self.edges['source'].str.contains('Compound') & self.edges['target'].str.contains('Disease')]
        # print(drugsWithDisease)
        # map the <disease, countOfDrugsAssociatedWithDisease> pairs to a dictionary
        diseaseCount = {}
        for x in drugsWithDisease['target']:
            if x in diseaseCount:
                diseaseCount[x] += 1
            else:
                diseaseCount[x] = 1
        # print(len(diseaseCount))
        #keep the diseases with less than n drugs
        filteredDiseases = {k: v for k, v in diseaseCount.items() if v < n}        
        # map<
        # key: value of filteredDiseases, which is number of drugs associated with each disease,
        # value: count
        # > to a dictionary
        filteredDiseasesCount = {}
        for x in filteredDiseases.values():
            if x in filteredDiseasesCount:
                filteredDiseasesCount[x] += 1
            else:
                filteredDiseasesCount[x] = 1
       
        # sort the dictionary by value in descending order
        sortedFilteredDiseasesCount = sorted(filteredDiseasesCount.items(), key=lambda x: x[1], reverse=True)
        print(f"\n\n\n\n\nQuery 2: Top 5 diseases with less than {n} drugs associated")
        print("-" * 60)
        for drug_count, disease_count in sortedFilteredDiseasesCount[:5]:
            print(f"{drug_count} drugs -> {disease_count} diseases")


        
        

