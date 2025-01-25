import pandas as pd
import tkinter as tk
from tkinter import messagebox

def view_results():
    try:
        # Read the Excel file
        df = pd.read_excel('users.xlsx')
        
        # Get the vote counts
        results = df['Vote'].value_counts()
        
        # Create the GUI window
        window = tk.Tk()
        window.title("Voting Results")
        
        # Create labels to display the results
        party_a_votes = results.get('Party A', 0)
        party_b_votes = results.get('Party B', 0)
        
        label = tk.Label(window, text=f"Voting Results\n\nParty A: {party_a_votes} votes\nParty B: {party_b_votes} votes",
                         font=("Arial", 14), padx=20, pady=20)
        label.pack()
        
        # Add a close button
        close_button = tk.Button(window, text="Close", command=window.destroy)
        close_button.pack(pady=10)
        
        # Start the Tkinter event loop
        window.mainloop()

    except Exception as e:
        # Show an error message in case of issues (e.g., file not found)
        messagebox.showerror("Error", f"An error occurred: {e}")

# Call the function to display the results
view_results()
