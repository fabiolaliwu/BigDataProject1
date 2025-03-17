from MongoDB import MongoDB
# from Neo4J import Neo4J
def display_menu():
    print("\n===== HetioNet Database Client =====")
    print("1. Get disease information (Query 1)")
    print("2. Find potential new drug-disease relationships (Query 2)")
    print("3. Exit")
    return input("Enter your choice (1-3): ")

#format list results for query 1
def format_list_result(items):
    if items and len(items) > 0:
        return ', '.join(items)
    else:
        return 'None'

if  __name__ == "__main__":
    print("Welcome to HetioNet Database Client")
    while True:
        choice = display_menu()
        mongo = MongoDB()
        
        if choice == '1':
            disease_id = input("Enter disease ID (ex: Disease::DOID:263): ")
            mongo.diseaseInfo(disease_id)   
        elif choice == '2':
            mongo.missingEdges()
        elif choice == '3':
            break  
        else:
            print("Invalid choice. Try Again")
    print("Connection closed")



    # testingMongoDB.diseaseInfo("Disease::DOID:4989")
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
    




