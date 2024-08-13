from flask import Flask, request, redirect, url_for, render_template, session
import pandas as pd
import uuid
import os
import secrets

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

filepath = 'users.xlsx'

def check_file_access(filepath):
    while True:
        try:
            with open(filepath, 'a'):
                break
        except IOError:
            time.sleep(4)

def init_db():
    if not os.path.exists(filepath):
        df = pd.DataFrame(columns=['Username', 'Password', 'GUID', 'Vote'])
        df.to_excel(filepath, index=False)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        check_file_access(filepath)
        df = pd.read_excel(filepath)
        if username in df['Username'].values:
            return "Username already exists. Please try again."
        guid = str(uuid.uuid4())
        new_user = pd.DataFrame([[username, password, guid, None]], columns=['Username', 'Password', 'GUID', 'Vote'])
        df = pd.concat([df, new_user], ignore_index=True)
        df.to_excel(filepath, index=False)
        return redirect(url_for('index'))
    return render_template('signup.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        check_file_access(filepath)
        df = pd.read_excel(filepath)
        user = df[(df['Username'] == username) & (df['Password'] == password)]
        if not user.empty:
            session['guid'] = user.iloc[0]['GUID']
            return redirect(url_for('vote'))
        return "Invalid username or password. Please try again."
    return render_template('login.html')

@app.route('/vote', methods=['GET', 'POST'])
def vote():
    if 'guid' not in session:
        return redirect(url_for('login'))

    if request.method == 'POST':
        guid = session['guid']
        choice = request.form['choice']
        check_file_access(filepath)
        df = pd.read_excel(filepath)
        user_index = df[df['GUID'] == guid].index
        current_vote = df.loc[user_index, 'Vote'].values[0]
        if current_vote and (current_vote == 'Party A' or current_vote == 'Party B'):
            return "You have already voted. You cannot vote again."

        if choice == '1':
            df.loc[user_index, 'Vote'] = 'Party A'
        elif choice == '2':
            df.loc[user_index, 'Vote'] = 'Party B'
        df.to_excel(filepath, index=False)
        return "Vote recorded successfully!"
    return render_template('vote.html')

@app.route('/logout')
def logout():
    session.pop('guid', None)
    return redirect(url_for('index'))

if __name__ == '__main__':
    init_db()
    app.run(debug=True)
