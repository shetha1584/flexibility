from flask import Flask, request
import psycopg2
from datetime import date

app = Flask(__name__)

PG_CONN = dict(
    host="localhost",
    dbname="Whatsapp_texts_sample",
    user="postgres",
    password="ABcd1234!@"
)

def get_consumer_id(phone):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute("SELECT id FROM consumers WHERE phone=%s", (phone,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

@app.route("/whatsapp", methods=["POST"])
def whatsapp_webhook():
    print("\n--- Incoming WhatsApp Webhook ---")
    print("Form data received:", request.form)  # shows all fields Twilio sent

    # Get phone number and message text
    from_no = request.form.get("From")
    from_no_clean = from_no.replace("whatsapp:", "").strip() if from_no else None
    message_text = request.form.get("Body") or request.form.get("ButtonText")

    print("Incoming number:", from_no_clean)
    print("Message text:", message_text)

    consumer_id = get_consumer_id(from_no_clean)
    print("Consumer ID:", consumer_id)

    # Debug: try to insert only if we have all info
    if consumer_id and message_text:
        shift_map = {
            "10%": 10,
            "25%": 25,
            "50%": 50,
            "Could not shift": 0
        }
        shifted = shift_map.get(message_text)
        print("Shifted percent:", shifted)

        if shifted is not None:
            try:
                conn = psycopg2.connect(**PG_CONN)
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO feedback (consumer_id, feedback_date, shifted_percent)
                    VALUES (%s, %s, %s)
                """, (consumer_id, date.today(), shifted))
                conn.commit()
                cur.close()
                conn.close()
                print("✅ Feedback inserted successfully")
            except Exception as e:
                print("❌ Error inserting feedback:", e)
        else:
            print("⚠ Message text not recognized in shift_map")
    else:
        print("⚠ Missing consumer ID or message text")

    return "OK"
