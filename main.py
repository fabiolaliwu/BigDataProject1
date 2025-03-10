from MongoDB import MongoDB
from Neo4J import Neo4J

if __name__ == "__main__":
    testingMongoDB = MongoDB()
    testingNeo4J = Neo4J()
    print("Loading nodes into MongoDB")
    testingMongoDB.cleanDatabase()  
    testingMongoDB.loadNodes()
    mongo_client = testingMongoDB.client
    mongo_db = mongo_client['hetio_database']
    nodes_collection = mongo_db['nodes'] 
    testingNeo4J.cleanDatabase()
    
   

    print("Done")



