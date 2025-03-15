from neo4j import GraphDatabase

#connection details
uri = 'bolt://localhost:7687'
username = 'neo4j'
password = 'huntercollege'

#connect to neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))
session = driver.session()

#query 1 <-- gets information about a disease (drugs, genes, and anatomical loc)
def get_query1(disease_id):
    query = """
        MATCH (d:Disease {id: $disease_id})
        OPTIONAL MATCH (drug:Compound)-[r1:RELATES {type: 'CtD'}]->(d)
        OPTIONAL MATCH (gene:Gene)-[r2:RELATES {type: 'DaG'}]->(d)
        OPTIONAL MATCH (d)-[r3:RELATES {type: 'DlA'}]->(anatomy:Anatomy)
        RETURN d.name AS Disease, 
               collect(DISTINCT drug.name) AS Drugs,
               collect(DISTINCT gene.name) AS Genes,
               collect(DISTINCT anatomy.name) AS Locations
    """

    #execute query
    result = session.run(query, disease_id=disease_id)
    return result.single()

#query 2 <-- finds potential new drug treatments by fiding compounds with opposite gene regulation effects
def get_query2():
    query = """
        MATCH (compound:Compound)-[:RELATES {type: 'CuG'}]->(gene:Gene)
        MATCH (disease:Disease)-[:RELATES {type: 'DlA'}]->(location:Anatomy)-[:RELATES {type: 'AdG'}]->(gene)
        WHERE NOT (compound)-[:RELATES {type: 'CtD'}]->(disease)
        RETURN compound.name AS Compound
        UNION
        MATCH (compound:Compound)-[:RELATES {type: 'CdG'}]->(gene:Gene)
        MATCH (disease:Disease)-[:RELATES {type: 'DlA'}]->(location:Anatomy)-[:RELATES {type: 'AuG'}]->(gene)
        WHERE NOT (compound)-[:RELATES {type: 'CtD'}]->(disease)
        RETURN compound.name AS Compound
        ORDER BY Compound
    """
    
    #execute query
    result = session.run(query)

    compounds = []

    #get compound names from query results
    for record in result:
        compound_name = record["Compound"]
        compounds.append(compound_name)

    return compounds

#menu for user
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

def main():
    print("Welcome to HetioNet Database Client")
    
    while True:
        #Display menu and get user choice
        choice = display_menu()
        
        if choice == '1':
            #query 1
            disease_id = input("Enter disease ID (ex: Disease::DOID:263): ")
            result = get_query1(disease_id)
            
            if result:
                print("\n----- RESULTS -----")
                print(f"Disease: {result['Disease']}")
                print(f"Drugs: {format_list_result(result['Drugs'])}")
                print(f"Genes: {format_list_result(result['Genes'])}")
                print(f"Locations: {format_list_result(result['Locations'])}")
            else:
                print(f"No disease found with ID: {disease_id}")
                
        elif choice == '2':
            #query 2
            compounds = get_query2()
            print("\n----- RESULTS -----")
            print(f"Found {len(compounds)} potential new treatment compounds:")
            counter = 1
            for compound in compounds:
                print(f"{counter}. {compound}")
                counter += 1
        elif choice == '3':
            #exit
            break
            
        else:
            print("Invalid choice. Try Again")
    
    #close connection
    session.close()
    driver.close()
    print("Connection closed")

if __name__ == "__main__":
    main()