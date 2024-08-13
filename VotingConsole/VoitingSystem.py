import pandas as pd
import uuid
import os
import time

def check_file_access(filepath):
    while True:
        try:
            with open(filepath, 'a'):
                break
        except IOError:
            print(f"Waiting for access to {filepath}...")
            time.sleep(4)

def sign_up():
    filepath = 'users.xlsx'
    check_file_access(filepath)
    
    if not os.path.exists(filepath):
        df = pd.DataFrame(columns=['Username', 'Password', 'GUID', 'Vote'])
        df.to_excel(filepath, index=False)

    df = pd.read_excel(filepath)

    username = input("Enter a username: ")
    password = input("Enter a password: ")

    if username in df['Username'].values:
        print("Username already exists. Please try again.")
        return

    guid = str(uuid.uuid4())

    new_user = pd.DataFrame([[username, password, guid, None]], columns=['Username', 'Password', 'GUID', 'Vote'])
    df = pd.concat([df, new_user], ignore_index=True)

    df.to_excel(filepath, index=False)

    print(f"User {username} registered successfully!")

def login():
    filepath = 'users.xlsx'
    check_file_access(filepath)
    
    if not os.path.exists(filepath):
        print("No users found. Please sign up first.")
        return None

    df = pd.read_excel(filepath)

    username = input("Enter your username: ")
    password = input("Enter your password: ")


    user = df[(df['Username'] == username) & (df['Password'] == password)]

    if not user.empty:
        guid = user.iloc[0]['GUID']
        #print(f"Login successful! Your session token: {guid}")
        return guid
    else:
        print("Invalid username or password. Please try again.")
        return login()

def vote(guid):
    filepath = 'users.xlsx'
    check_file_access(filepath)
    
    # Load the user data from the Excel file
    df = pd.read_excel(filepath)

    # Ensure the Vote column is explicitly cast to string
    df['Vote'] = df['Vote'].astype(str)

    # Find the user by GUID
    user_index = df[df['GUID'] == guid].index

    if user_index.empty:
        print("User not found.")
        return

    # Check if the user has already voted
    current_vote = df.loc[user_index, 'Vote'].values[0]
    if current_vote and (current_vote == 'Party A' or current_vote == 'Party B'):
        print("You have already voted. You cannot vote again.")
        return

    print("\nVote for your preferred party:")
    print("1. Party A")
    print("2. Party B")
    choice = input("Enter the number corresponding to your choice: ")

    if choice == '1':
        df.loc[user_index, 'Vote'] = 'Party A'
        print("You voted for Party A.")
    elif choice == '2':
        df.loc[user_index, 'Vote'] = 'Party B'
        print("You voted for Party B.")
    else:
        return
    
    df.to_excel(filepath, index=False)

def main():
    while True:
        print("\n1. Sign Up")
        print("\n2. Login")
        print("\n3. Exit")
        choice = input("Choose an option: ")

        if choice == '1':
            sign_up()
        elif choice == '2':
            token = login()
            if token:
                vote(token)
        elif choice == '3':
            break
        else:
            print("Invalid option. Please choose again.")

if __name__ == "__main__":
    main()
