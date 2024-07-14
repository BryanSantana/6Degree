from flask import Flask, request, jsonify
import psycopg2
import os
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)

load_dotenv()
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
con = psycopg2.connect(user=user, password=password)
cur = con.cursor()

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data['username']
    password = data['password']
    
    cur.execute("SELECT password FROM users WHERE username = %s", (username,))
    user = cur.fetchone()
    if user and check_password_hash(user[0], password):
        return jsonify({"message": "Login successful"}), 200
    else:
        return jsonify({"message": "Invalid username or password"}), 401

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data['username']
    password = generate_password_hash(data['password'])
    
    cur.execute("INSERT INTO users (username, password) VALUES (%s, %s)", (username, password))
    con.commit()
    
    return jsonify({"message": "User registered successfully"}), 201

@app.route('/validate', methods=['POST'])
def validate():
    data = request.json
    start_player_id = data['start_player_id']
    end_player_id = data['end_player_id']
    guess = data['guess']
    
    start_player_teammates = get_player_teammates(start_player_id)
    end_player_teammates = get_player_teammates(end_player_id)
    common_teammates = start_player_teammates.intersection(end_player_teammates)
    
    if guess in common_teammates:
        return jsonify({"message": "Correct"}), 200
    else:
        return jsonify({"message": "Incorrect"}), 400

def get_player_teammates(player_id):
    cur.execute("SELECT team_id, start_date, end_date FROM player_team_join WHERE player_id = %s", (player_id,))
    data = cur.fetchall()

    teammate_name = set()
    for entry in data:
        team_id, start_date, end_date = entry
        cur.execute("""
            SELECT p.name
            FROM player_team_join ptj
            JOIN MLB_Players p ON ptj.player_id = p.player_id
            WHERE ptj.team_id = %s AND ptj.start_date <= %s AND ptj.end_date >= %s AND ptj.player_id != %s
        """, (team_id, end_date, start_date, player_id))
        teammates = cur.fetchall()
        for entry in teammates:
            teammate_name.add(entry[0])
    return teammate_name

if __name__ == '__main__':
    app.run(debug=True)
