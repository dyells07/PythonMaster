import pandas as pd

def view_results():
    df = pd.read_excel('users.xlsx')
    results = df['Vote'].value_counts()
    print("\nVoting Results:")
    print(f"Party A: {results.get('Party A', 0)} votes")
    print(f"Party B: {results.get('Party B', 0)} votes")


view_results()