import pandas as pd

def view_results():
    try:
        df = pd.read_excel('users.xlsx', engine='openpyxl')  # Use 'openpyxl' for better performance
        if 'Vote' not in df.columns:
            print("No voting data found.")
            return

        results = df['Vote'].value_counts().to_dict()  # Convert to dictionary for better handling
        print("\nVoting Results:")
        print(f"Party A: {results.get('Party A', 0)} votes")
        print(f"Party B: {results.get('Party B', 0)} votes")
    
    except FileNotFoundError:
        print("Error: users.xlsx file not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function
view_results()
