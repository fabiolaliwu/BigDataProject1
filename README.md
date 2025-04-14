# Big Data Technology

## Project 1
The HetioNet is a tool to understand the complex relationships between diseases, genes, compounds, and anatomy. This project focuses on building a database to efficiently handle queries related to disease location, genes, and treatment by utilizing MongoDB and Neo4J NoSQL data stores.
You can either cd to the mongodb or neo4j folder in order to run the query

For mongodb:
```python main.py```

For neo4j:
```python hetionet_neo4j_client.py ``

#### Query 1: 
Given a disease id, it will print out the name, related drugs, genes, and anatomy.

#### Query 2:
We assume that a compound can treat a disease if the compound up-regulates a gene(CuG), but the location down-regulates the gene in an opposite direction where the disease occurs(AdG), and if the compound down-regulates a gene(CdG), but the location up-regulates (AuG) the gene in an opposite direction where the disease occurs. It finds all compounds that can treat a new disease (i.e. the missing edges between compound and disease excluding existing drugs). 

## Project 2
This is a continuation of Project1. In this project we will use the algorithm of mapreduce in order to improve efficiency.
#### Query 1: 
Computes the number of genes and diseases associated with each drug in `nodes.tsv`, and outputs the top 5 drugs with the most genes in descending order.
1. Filter all the edges with source = ‘Compund’ 
2. Filter all the genes and diseases from step 1.
3. Map every compound <compoundID, 1>
4. Group the maps by the compoundID e.g. <compundID, genesAmount> and  <compundID, diseaseAmount>
5. Sort the map by its values.
6. Print out the first 5 compoundID, amount of genes, and amount of diseases associated with the drug(compound)


#### Query 2:
Computes the number of diseases associated with up to **n** drugs, and outputs the top 5 diseases in descending order.
1. Filter all the compounds that have disease in their target.
2. Map <disease, drugsAmount> e.g: disease1 has 4 drugs related to it -> <disease1, 4>
3. Filter out the diseases with amount greater than n.
4. Map <drugAmount, drugAmountAmount>. For example: <disease1, 4>, <disease4, 4>, <disease10, 4> -> <4, 3>
5. Print out the top 5.


#### Query 3:
1. Retrieves the names of drugs with the top 5 highest gene associations.
2. Filter all the edges with source = ‘Compund’ 
3. Filter all the genes and diseases from step 1.
4. Map every compound <compoundID, 1>
5. Group the maps by the compoundID e.g. <compundID, genesAmount> 
6. Sort the map by its values.
7. Print out the first 5 compound name by getting it from the nodes.tsv file, and the amount of genes.

