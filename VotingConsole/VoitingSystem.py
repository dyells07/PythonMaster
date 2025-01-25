import pandas as pd
import uuid
import os
import time
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from threading import Thread

# Utility function to check file access
def check_file_access(filepath):
    while True:
        try:
            with open(filepath, 'a'):
                break
        except IOError:
            time.sleep(4)

# Function to initialize the data file
def initialize_data_file():
    filepath = 'users.xlsx'
    if not os.path.exists(filepath):
        df = pd.DataFrame(columns=['Username', 'Password', 'GUID', 'Vote', 'Role', 'RegisteredOn'])
        df.to_excel(filepath, index=False)
    return filepath

# Function to handle signup
def sign_up():
    filepath = initialize_data_file()
    df = pd.read_excel(filepath)

    username = simpledialog.askstring("Sign Up", "Enter a username:")
    if not username:
        return

    if username in df['Username'].values:
        messagebox.showerror("Error", "Username already exists. Please try again.")
        return

    password = simpledialog.askstring("Sign Up", "Enter a password:", show='*')
    if not password:
        return

    guid = str(uuid.uuid4())
    registered_on = time.strftime("%Y-%m-%d %H:%M:%S")

    new_user = pd.DataFrame(
        [[username, password, guid, None, 'User', registered_on]],
        columns=['Username', 'Password', 'GUID', 'Vote', 'Role', 'RegisteredOn']
    )
    df = pd.concat([df, new_user], ignore_index=True)
    df.to_excel(filepath, index=False)

    messagebox.showinfo("Success", f"User {username} registered successfully!")

# Function to handle login
def login(role_required='User'):
    filepath = initialize_data_file()
    df = pd.read_excel(filepath)

    username = simpledialog.askstring("Login", "Enter your username:")
    if not username:
        return None

    password = simpledialog.askstring("Login", "Enter your password:", show='*')
    if not password:
        return None

    user = df[(df['Username'] == username) & (df['Password'] == password)]
    if user.empty:
        messagebox.showerror("Error", "Invalid username or password.")
        return None

    if user.iloc[0]['Role'] != role_required and role_required != 'User':
        messagebox.showerror("Error", f"Access denied. {role_required} role required.")
        return None

    messagebox.showinfo("Success", "Login successful!")
    return user.iloc[0]['GUID']

# Function to handle voting
def vote(guid):
    filepath = initialize_data_file()
    df = pd.read_excel(filepath)
    user_index = df[df['GUID'] == guid].index

    if user_index.empty:
        messagebox.showerror("Error", "User not found.")
        return

    current_vote = df.loc[user_index, 'Vote'].values[0]
    if current_vote:
        messagebox.showinfo("Info", f"You have already voted for {current_vote}.")
        return

    def cast_vote(choice):
        nonlocal df, user_index
        df.loc[user_index, 'Vote'] = choice
        df.to_excel(filepath, index=False)
        messagebox.showinfo("Success", f"You voted for {choice}!")
        vote_window.destroy()

    vote_window = tk.Toplevel()
    vote_window.title("Vote")
    vote_window.geometry("300x200")
    tk.Label(vote_window, text="Vote for your preferred party:", font=("Arial", 14)).pack(pady=10)
    tk.Button(vote_window, text="Party A", command=lambda: cast_vote('Party A'), width=15).pack(pady=5)
    tk.Button(vote_window, text="Party B", command=lambda: cast_vote('Party B'), width=15).pack(pady=5)

# Function to view real-time results
def view_results():
    filepath = initialize_data_file()
    
    def update_results():
        while True:
            time.sleep(1)  # Refresh every second
            df = pd.read_excel(filepath)
            results = df['Vote'].value_counts()
            party_a_votes.set(f"Party A: {results.get('Party A', 0)} votes")
            party_b_votes.set(f"Party B: {results.get('Party B', 0)} votes")

    results_window = tk.Toplevel()
    results_window.title("Live Voting Results")
    results_window.geometry("300x200")

    tk.Label(results_window, text="Live Voting Results", font=("Arial", 16, "bold")).pack(pady=10)
    party_a_votes = tk.StringVar(value="Party A: 0 votes")
    party_b_votes = tk.StringVar(value="Party B: 0 votes")
    tk.Label(results_window, textvariable=party_a_votes, font=("Arial", 14)).pack(pady=5)
    tk.Label(results_window, textvariable=party_b_votes, font=("Arial", 14)).pack(pady=5)

    # Run the update function in a separate thread
    Thread(target=update_results, daemon=True).start()

# Main GUI application
def main():
    root = tk.Tk()
    root.title("Voting System")
    root.geometry("400x400")

    tk.Label(root, text="Welcome to the Voting System", font=("Arial", 18, "bold")).pack(pady=20)
    tk.Button(root, text="Sign Up", command=sign_up, width=20).pack(pady=10)
    tk.Button(root, text="Login and Vote", command=lambda: login_and_vote(), width=20).pack(pady=10)
    tk.Button(root, text="View Results (Live)", command=view_results, width=20).pack(pady=10)
    tk.Button(root, text="Exit", command=root.quit, width=20).pack(pady=10)

    def login_and_vote():
        guid = login()
        if guid:
            vote(guid)

    root.mainloop()

if __name__ == "__main__":
    initialize_data_file()
    main()
