from MongoDB import MongoDB
import tkinter as tk
from tkinter import simpledialog, scrolledtext

#format list results for query 1
def format_list_result(items):
    if items and len(items) > 0:
        return ', '.join(items)
    else:
        return 'None'

class HetioNetGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("HetioNet Database Client")
        self.root.geometry("600x500")
        self.mongo = MongoDB()
        
        #title
        title_label = tk.Label(root, text="HetioNet Database Client", font=("Arial", 14))
        title_label.pack(pady=10)
        
        #menu buttons frame
        menu_frame = tk.Frame(root)
        menu_frame.pack(pady=10)
        
        #menu buttons
        btn1 = tk.Button(menu_frame, text="1. Get disease information (Query 1)", 
                         width=40, command=self.choice_1)
        btn1.pack(pady=5)
        
        btn2 = tk.Button(menu_frame, text="2. Find potential new drug-disease relationships (Query 2)", 
                         width=40, command=self.choice_2)
        btn2.pack(pady=5)
        
        #results display
        results_label = tk.Label(root, text="Results:")
        results_label.pack(anchor=tk.W, padx=10)
        
        self.output_text = scrolledtext.ScrolledText(root, height=20, width=70)
        self.output_text.pack(padx=10, pady=5, fill=tk.BOTH, expand=True)
        
        #window close event
        self.root.protocol("WM_DELETE_WINDOW", self.close_window)
    
    def choice_1(self):
        #prompt for disease ID
        disease_id = simpledialog.askstring("Input", "Enter disease ID (ex: Disease::DOID:263):", 
                                           initialvalue="Disease::DOID:263")
        if not disease_id:
            return
        
        #clear output text
        self.output_text.delete(1.0, tk.END)
        
        #execute query and get results
        result = self.mongo.diseaseInfo(disease_id)
        
        if result:
            self.output_text.insert(tk.END, f"Disease: {result['Disease']}\n")
            self.output_text.insert(tk.END, f"Drugs: {format_list_result(result['Drugs'])}\n")
            self.output_text.insert(tk.END, f"Genes: {format_list_result(result['Genes'])}\n")
            self.output_text.insert(tk.END, f"Locations: {format_list_result(result['Locations'])}\n")
        else:
            self.output_text.insert(tk.END, f"No disease found with ID: {disease_id}")
    
    def choice_2(self):
        #clear output text
        self.output_text.delete(1.0, tk.END)
        
        #show "Processing..." message
        self.output_text.insert(tk.END, "Processing query... This may take a moment.\n")
        self.root.update_idletasks()
        
        #execute query and get results
        results = self.mongo.missingEdges()
        
        #clear output and display results
        self.output_text.delete(1.0, tk.END)
        self.output_text.insert(tk.END, f"Found {len(results)} potential new treatment compounds:\n")
        
        #display each compound in the same format as Neo4j
        counter = 1
        for result in results:
            self.output_text.insert(tk.END, f"{counter}. {result['compound_name']}\n")
            counter += 1
    
    def close_window(self):
        #close connection
        print("Connection closed")
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = HetioNetGUI(root)
    root.mainloop()