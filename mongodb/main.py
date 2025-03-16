from MongoDB import MongoDB
# from Neo4J import Neo4J

if __name__ == "__main__":
    # print("\n\n======MAIN MENU===== \nChoose one of the following options: \n")
    # print("1. Get disease Information \n2. Find missing CtD edges \n3. EXIT\n")
    # option = input()
    # if option != 1 or 2 or 3:
    #     print("inavlid option")
    
    # print("You chose option ", option)
    
    testingMongoDB = MongoDB()
    testingMongoDB.diseaseInfo("Disease::DOID:4989")
    #already in DataBase so do not mess up by uncommenting it
    #print("Loading nodes into MongoDB")
    # testingMongoDB.cleanDatabase()  
    # testingMongoDB.loadNodes()
    # testingMongoDB.loadEdges()
    # mongo_client = testingMongoDB.client
    # mongo_db = mongo_client['hetio_database']
    # nodes_collection = mongo_db['nodes'] 

    # disease_id = "Disease::DOID:1094"
    # testingMongoDB.diseaseInfo(disease_id)

    # testingMongoDB.diseaseInfo("Disease::DOID:0050156")
    #testingMongoDB.findMatchingEdges()
    # testingMongoDB.findMatchingEdges()

    # testingMongoDB.checkEdges()
    # testingMongoDB.checkAnatomyEdges()
    # testingMongoDB.testDatabase()


    # testingNeo4J = Neo4J()
    # testingNeo4J.cleanDatabase()
    




