from flask import Flask, request, redirect, url_for, render_template, session
import pandas as pd
import uuid
import os
import secrets
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

FILEPATH = 'users.xlsx'
COLUMNS = ['Username', 'Password', 'GUID', 'Vote']

# Initialize database
def init_db():
    if not os.path.exists(FILEPATH):
        df = pd.DataFrame(columns=COLUMNS)
        df.to_excel(FILEPATH, index=False, engine='openpyxl')

# Load user data
def load_users():
    return pd.read_excel(FILEPATH, engine='openpyxl')

# Save user data
def save_users(df):
    df.to_excel(FILEPATH, index=False, engine='openpyxl')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        df = load_users()
        if username in df['Username'].values:
            return "Username already exists. Please try again."

        hashed_password = generate_password_hash(password)
        new_user = pd.DataFrame([[username, hashed_password, str(uuid.uuid4()), None]], columns=COLUMNS)
        df = pd.concat([df, new_user], ignore_index=True)
        save_users(df)
        
        return redirect(url_for('index'))
    return render_template('signup.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        df = load_users()
        user = df[df['Username'] == username]
        
        if not user.empty and check_password_hash(user.iloc[0]['Password'], password):
            session['guid'] = user.iloc[0]['GUID']
            return redirect(url_for('vote'))
        return "Invalid username or password. Please try again."
    
    return render_template('login.html')

@app.route('/vote', methods=['GET', 'POST'])
def vote():
    if 'guid' not in session:
        return redirect(url_for('login'))

    df = load_users()
    user_index = df[df['GUID'] == session['guid']].index

    if user_index.empty:
        return redirect(url_for('logout'))  # Log out if session is invalid

    if request.method == 'POST':
        if df.loc[user_index, 'Vote'].notna().any():
            return "You have already voted. You cannot vote again."
        
        choice = request.form['choice']
        df.loc[user_index, 'Vote'] = 'Party A' if choice == '1' else 'Party B'
        save_users(df)

        return "Vote recorded successfully!"
    
    return render_template('vote.html')

@app.route('/logout')
def logout():
    session.pop('guid', None)
    return redirect(url_for('index'))

if __name__ == '__main__':
    init_db()
    app.run(debug=True)
