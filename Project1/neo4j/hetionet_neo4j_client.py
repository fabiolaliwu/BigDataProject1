import tkinter as tk
from tkinter import simpledialog, scrolledtext
from neo4j import GraphDatabase


#connection details
uri = 'bolt://localhost:7687'
username = 'neo4j'
password = 'huntercollege'

#connect to neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))
session = driver.session()

#Query 1
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

#query 2
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

#format list results
def format_list_result(items):
    if items and len(items) > 0:
        return ', '.join(items)
    else:
        return 'None'

#GUI realted code
#menu choice
def choice_1():
    #prompt for disease ID
    disease_id = simpledialog.askstring("Input", "Enter disease ID (ex: Disease::DOID:263):", 
                                       initialvalue="Disease::DOID:263")
    if not disease_id:
        return
    
    result = get_query1(disease_id)
    
    output_text.delete(1.0, tk.END)
    if result:
        output_text.insert(tk.END, f"Disease: {result['Disease']}\n")
        output_text.insert(tk.END, f"Drugs: {format_list_result(result['Drugs'])}\n")
        output_text.insert(tk.END, f"Genes: {format_list_result(result['Genes'])}\n")
        output_text.insert(tk.END, f"Locations: {format_list_result(result['Locations'])}\n")
    else:
        output_text.insert(tk.END, f"No disease found with ID: {disease_id}")

def choice_2():
    compounds = get_query2()
    
    output_text.delete(1.0, tk.END)
    output_text.insert(tk.END, f"Found {len(compounds)} potential new treatment compounds:\n")
    
    counter = 1
    for compound in compounds:
        output_text.insert(tk.END, f"{counter}. {compound}\n")
        counter += 1

def close_window():
    #close connection
    session.close()
    driver.close()
    print("Connection closed")
    root.destroy()

#create the main window
root = tk.Tk()
root.title("HetioNet Database Client")
root.geometry("600x500")

#title
title_label = tk.Label(root, text="HetioNet Database Client", font=("Arial", 14))
title_label.pack(pady=10)

#menu buttons frame
menu_frame = tk.Frame(root)
menu_frame.pack(pady=10)

#menu buttons
btn1 = tk.Button(menu_frame, text="1. Get disease information (Query 1)", width=40, command=choice_1)
btn1.pack(pady=5)

btn2 = tk.Button(menu_frame, text="2. Find potential new drug-disease relationships (Query 2)", width=40, command=choice_2)
btn2.pack(pady=5)

#results display
results_label = tk.Label(root, text="Results:")
results_label.pack(anchor=tk.W, padx=10)

output_text = scrolledtext.ScrolledText(root, height=20, width=70)
output_text.pack(padx=10, pady=5, fill=tk.BOTH, expand=True)

#handle window close event
root.protocol("WM_DELETE_WINDOW", close_window)

#start GUI
root.mainloop()