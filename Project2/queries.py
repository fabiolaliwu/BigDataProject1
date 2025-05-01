from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

class Queries:
    def __init__(self):
        self.spark = SparkSession.builder.appName("CompoundQueries").getOrCreate()
        self.nodes = self.spark.read.csv('data/nodes.tsv', sep='\t', header=True, inferSchema=True)
        self.edges = self.spark.read.csv('data/edges.tsv', sep='\t', header=True, inferSchema=True)

    def query(self, queryNumber):
        compounds = self.edges.filter(col('source').contains('Compound'))
        genesAssociated = compounds.filter(col('target').contains('Gene'))
        diseaseAssociated = compounds.filter(col('target').contains('Disease'))
        genesCountDF = genesAssociated.groupBy('source').agg(count('*').alias('gene_count'))
        diseaseCountDF = diseaseAssociated.groupBy('source').agg(count('*').alias('disease_count'))

        joinedCounts = genesCountDF.join(diseaseCountDF, on='source', how='left').fillna(0)
        sortedGenes = joinedCounts.orderBy(desc('gene_count'))
        if queryNumber == 1:
            print(f"\n\n\n\n\nQuery 1: Top 5 compounds with most genes associated, and their associated diseases")
            print(f"{'Compound ID':<20}{'Genes':<10}{'Diseases':<10}")
            print("-" * 40)
            top5 = sortedGenes.limit(5).collect()
            for row in top5:
                print(f"{row['source']:<20}{row['gene_count']:<10}{int(row['disease_count']):<10}")
        elif queryNumber == 3:
            print(f"\n\n\n\n\nQuery 3: Names of top 5 compounds with most genes associated")
            print("-" * 60)
            top5 = sortedGenes.limit(5)
            top5WithNames = top5.join(self.nodes, top5.source == self.nodes.id, how='left')
            for row in top5WithNames.select('name', 'gene_count').collect():
                print(f"{row['name']} -> {row['gene_count']}")

    def query2(self, n):
        drugsWithDisease = self.edges.filter(
            col('source').contains('Compound') & col('target').contains('Disease')
        )
        diseaseCounts = drugsWithDisease.groupBy('target').agg(count('source').alias('drug_count'))
        filtered = diseaseCounts.filter(col('drug_count') < n)
        distribution = filtered.groupBy('drug_count').agg(count('*').alias('disease_count'))
        sortedDistribution = distribution.orderBy(desc('disease_count'))
        print(f"\n\n\n\n\nQuery 2: Top 5 diseases with less than {n} drugs associated")
        print("-" * 60)
        for row in sortedDistribution.limit(5).collect():
            print(f"{row['drug_count']} drugs -> {row['disease_count']} diseases")
