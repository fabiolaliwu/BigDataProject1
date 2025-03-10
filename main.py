from MongoDB import MongoDB
from Neo4J import Neo4J

if __name__ == "__main__":
    testingMongoDB = MongoDB()
    print("Loading nodes into MongoDB")
    #already in DataBase so do not mess up by uncommenting it
    # testingMongoDB.cleanDatabase()  
    # testingMongoDB.loadNodes()
    # testingMongoDB.loadEdges()
    # mongo_client = testingMongoDB.client
    # mongo_db = mongo_client['hetio_database']
    # nodes_collection = mongo_db['nodes'] 

    # testingNeo4J = Neo4J()
    # testingNeo4J.cleanDatabase()
    
    print("Done")



