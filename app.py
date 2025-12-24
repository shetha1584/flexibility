from flask import Flask, request
import psycopg2
import os
from datetime import datetime

app = Flask(__name__)

DB_URL = os.environ["DATABASE_URL"]

def save_response(client_id, shift):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO load_shift_responses (client_id, shift_percent, response_time)
        VALUES (%s, %s, %s)
    """, (client_id, shift, datetime.utcnow()))

    conn.commit()
    cur.close()
    conn.close()

@app.route("/track")
def track():
    client_id = request.args.get("client_id")
    shift = request.args.get("shift")

    save_response(client_id, shift)

    return f"""
    <html>
      <body style="font-family: Arial; text-align:center; margin-top:60px;">
        <h2>✅ Response Recorded</h2>
        <p>{shift}% load shift confirmed.</p>
      </body>
    </html>
    """

@app.route("/")
def home():
    return "Server running"
