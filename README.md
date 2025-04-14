# Big Data Technology

## Project 1
The HetioNet is a tool to understand the complex relationships between diseases, genes, compounds, and anatomy. This project focuses on building a database to efficiently handle queries related to disease location, genes, and treatment by utilizing MongoDB and Neo4J NoSQL data stores.


## Project 2
This is a continuation of Project1. In this project we will use the algorithm of mapreduce in order to improve efficiency.
#### Query 1: 
Computes the number of genes and diseases associated with each drug in `nodes.tsv`, and outputs the top 5 drugs with the most genes in descending order.
1. Filter all the edges with source = ‘Compund’ 
2. Filter all the genes and diseases from step 1.
3. Map every compound <compoundID, 1>
4. Group the maps by the compoundID e.g. <compundID, genesAmount> and  <compundID, diseaseAmount>
5. Sort the map by its values.
6. Prints out the first 5 compoundID, amount of genes, and amount of diseases associated with the drug(compound)


#### Query 2:
Computes the number of diseases associated with up to **n** drugs, and outputs the top 5 diseases in descending order.

#### Query 3:
- Retrieves the names of drugs with the top 5 highest gene associations.
