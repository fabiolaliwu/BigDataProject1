from neo4j import GraphDatabase
import time

#Neo4j connection details
uri = 'bolt://localhost:7687'
username = 'neo4j'
password = 'huntercollege'

#connect to Neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))
session = driver.session()

print("Clearing database...")
#clearing database by deleting all nodes and their relationships from the database
session.run("MATCH (n) DETACH DELETE n")
print("Database cleared")

#import all nodes
print("Importing nodes...")
nodes_start_time = time.time()  #start timing node import

#creates nodes from the TSV file, giving each one an id, name, and kind
node_query = """
LOAD CSV WITH HEADERS FROM 'file:///nodes.tsv' AS row FIELDTERMINATOR '\t'
CREATE (n:Node {id: row.id, name: row.name, kind: row.kind})
"""
session.run(node_query, database_="neo4j", config={"transaction.timeout": 600})

#finds nodes of each specific kind and adds an additional label to make them easier to query
session.run("MATCH (n:Node) WHERE n.kind = 'Compound' SET n:Compound")
session.run("MATCH (n:Node) WHERE n.kind = 'Disease' SET n:Disease") 
session.run("MATCH (n:Node) WHERE n.kind = 'Gene' SET n:Gene")
session.run("MATCH (n:Node) WHERE n.kind = 'Anatomy' SET n:Anatomy")

#creates a lookup index on the id property to make finding nodes faster
session.run("CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.id)")

nodes_elapsed = time.time() - nodes_start_time  #calculate elapsed time
print(f"Nodes imported in {nodes_elapsed:.2f} seconds")

#counts the total number of edges we need to import
result = session.run("LOAD CSV WITH HEADERS FROM 'file:///edges.tsv' AS row FIELDTERMINATOR '\t' RETURN count(*) as count")
edge_count = result.single()["count"]

print("Importing edges in batches (may take longer)...")
edges_start_time = time.time()  #start timing edge import

batch_size = 50000 #batch size
skip = 0

#processing edges in batches
while skip < edge_count:
    #for DaG edges (flipping direction)
    dag_query = f"""
    LOAD CSV WITH HEADERS FROM 'file:///edges.tsv' AS row FIELDTERMINATOR '\t'
    SKIP {skip} LIMIT {batch_size}
    MATCH (source:Node {{id: row.source}})
    MATCH (target:Node {{id: row.target}})
    WHERE row.metaedge = 'DaG'
    CREATE (target)-[r:RELATES {{type: row.metaedge}}]->(source)
    """

    #for all other edge types
    other_query = f"""
    LOAD CSV WITH HEADERS FROM 'file:///edges.tsv' AS row FIELDTERMINATOR '\t'
    SKIP {skip} LIMIT {batch_size}
    MATCH (source:Node {{id: row.source}})
    MATCH (target:Node {{id: row.target}})
    WHERE row.metaedge <> 'DaG'
    CREATE (source)-[r:RELATES {{type: row.metaedge}}]->(target)
    """
    
    #run both queries for this batch
    session.run(dag_query, database_="neo4j", config={"transaction.timeout": 600})
    session.run(other_query, database_="neo4j", config={"transaction.timeout": 600})
    
    skip += batch_size

edges_elapsed = time.time() - edges_start_time  #calculate elapsed time
print(f"Edges imported in {edges_elapsed:.2f} seconds")

#close connection
session.close()
driver.close()
print("Import complete!")