import psycopg2
from datetime import datetime
import pytz
import time
import pywhatkit as kit

# ----------------- YOUR WHATSAPP NUMBER -----------------
MY_WHATSAPP_NUMBER = "+917812840850"   # <-- your number here (with +91)

# ----------------- POSTGRES CONFIG -----------------
PG_CONN = dict(
    host="localhost",
    dbname="whatsapp_texts_sample",
    user="postgres",
    password="ABcd1234!@"
)

# ----------------- TIME HELPERS -----------------
def get_ist_now():
    utc_now = datetime.utcnow()
    ist = pytz.timezone("Asia/Kolkata")
    return utc_now.replace(tzinfo=pytz.utc).astimezone(ist)

# ----------------- DB FETCH -----------------
def fetch_pending_messages(conn, current_time):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            c.client_id,
            c.phone,
            m.message_id,
            m.message_text
        FROM clients c
        JOIN daily_messages m ON m.active = TRUE
        WHERE c.active = TRUE
          AND m.send_time = %s
          AND NOT EXISTS (
              SELECT 1
              FROM message_log l
              WHERE l.client_id = c.client_id
                AND l.message_id = m.message_id
                AND DATE(l.sent_at) = CURRENT_DATE
          )
    """, (current_time,))
    rows = cur.fetchall()
    cur.close()
    return rows

# ----------------- LOG RESULT -----------------
def log_message(conn, client_id, message_id, status, error=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO message_log (client_id, message_id, status, error)
        VALUES (%s, %s, %s, %s)
    """, (client_id, message_id, status, error))
    conn.commit()
    cur.close()

# ----------------- MAIN LOOP -----------------
def main():
    print("[INFO] WhatsApp nudge service started")

    conn = None

    while True:
        if conn is None or conn.closed:
            conn = psycopg2.connect(**PG_CONN)

        ist_now = get_ist_now()
        current_time = ist_now.strftime("%H:%M:%S")
        print(f"[INFO] Checking messages for {current_time} IST")

        messages = fetch_pending_messages(conn, current_time)

        for client_id, phone, message_id, message_text in messages:
            # Normalize phone number
            phone = phone if phone.startswith("+") else "+91" + phone

            # 🚫 Skip sending to yourself
            if phone == MY_WHATSAPP_NUMBER:
                print(f"[SKIP] Skipping self message to {phone}")
                log_message(conn, client_id, message_id, "SKIPPED", "Self number")
                continue

            print(f"[INFO] Sending message to {phone}")

            try:
                kit.sendwhatmsg_instantly(
                    phone,
                    message_text,
                    wait_time=10,
                    tab_close=False
                )
                log_message(conn, client_id, message_id, "SENT")
                print(f"[OK] Sent to {phone}")

            except Exception as e:
                log_message(conn, client_id, message_id, "FAILED", str(e))
                print(f"[ERROR] Failed for {phone}: {e}")

            time.sleep(5)  # anti-spam delay

        time.sleep(60)  # check every minute

# ----------------- ENTRY -----------------
if __name__ == "__main__":
    main()
