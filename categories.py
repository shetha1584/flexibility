import threading
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values
import psycopg2

# --------------------------------------------------
# DATABASE CONFIG
# --------------------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "dbname": "elements_flex",
    "user": "postgres",
    "password": "ABcd1234!@",
    "port": 5432
}

MAX_WORKERS = 10
lock = threading.Lock()


# --------------------------------------------------
# DB CONNECTION
# --------------------------------------------------
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# --------------------------------------------------
# FETCH CLIENT LIST
# --------------------------------------------------
def fetch_clients():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT scno, short_name FROM clients;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# --------------------------------------------------
# FETCH CONSUMPTION FOR ONE CLIENT
# --------------------------------------------------
def fetch_consumption(scno, start, end):
    """Fetch consumption from API or other source. Dummy for now."""
    return []


# --------------------------------------------------
# CREATE NEW CATEGORIES TABLE
# --------------------------------------------------
def create_category_table():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS client_categories (
            scno VARCHAR PRIMARY KEY,
            name VARCHAR,
            avg_consumption DOUBLE PRECISION,
            variability DOUBLE PRECISION,
            consumption_level VARCHAR,
            variability_level VARCHAR,
            final_category VARCHAR,
            calculated_at TIMESTAMP DEFAULT NOW()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()


# --------------------------------------------------
# CATEGORY RULES
# --------------------------------------------------
def get_consumption_level(avg_c):
    if avg_c >= 200:
        return "High"
    elif avg_c >= 50:
        return "Medium"
    else:
        return "Low"


def get_variability_level(cv):
    """CV thresholds: >40% = High variability (tweakable)"""
    if cv >= 40:
        return "High"
    else:
        return "Low"


# --------------------------------------------------
# PROCESS A SINGLE CLIENT
# --------------------------------------------------
def process_client(scno, name, fetch_start, fetch_end, conn):
    try:
        cur = conn.cursor()

        # Fetch missing consumption data
        new_data = fetch_consumption(scno, fetch_start, fetch_end)

        if new_data:
            execute_values(cur, """
                INSERT INTO consumption (scno, date, hour, consumption)
                VALUES %s
                ON CONFLICT (scno, date, hour)
                DO UPDATE SET consumption = EXCLUDED.consumption;
            """, new_data)
            conn.commit()

            with lock:
                print(f"✅ {name} ({scno}) — data updated.")
        else:
            with lock:
                print(f"⏩ {name} ({scno}) — up-to-date.")

        # Load consumption for stats
        df = pd.read_sql(
            "SELECT consumption FROM consumption WHERE scno=%s;",
            conn, params=(scno,)
        )

        if df.empty:
            return None

        # Compute stats
        avg_c = float(df["consumption"].mean())
        sd_c = float(df["consumption"].std() if len(df) > 1 else 0.0)

        # ---- FIXED: CV instead of SD ----
        cv = float((sd_c / avg_c) * 100) if avg_c != 0 else 0.0

        cons_level = get_consumption_level(avg_c)
        var_level = get_variability_level(cv)

        final_cat = f"{cons_level} Consumer — {var_level} Variability"

        return {
            "scno": scno,
            "name": name,
            "avg_consumption": avg_c,
            "variability": cv,        # Store CV, not SD
            "consumption_level": cons_level,
            "variability_level": var_level,
            "final_category": final_cat
        }

    except Exception as e:
        print(f"❌ Error {scno}: {e}")
        return None

    finally:
        conn.close()


# --------------------------------------------------
# MAIN SCRIPT
# --------------------------------------------------
def main():
    create_category_table()

    conn = get_conn()
    cur = conn.cursor()

    clients = fetch_clients()
    if not clients:
        print("No clients found.")
        return

    end_date = datetime.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=60)

    print(f"\n🚀 Processing {len(clients)} clients...\n")

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                process_client, scno, name, start_date, end_date, get_conn()
            )
            for scno, name in clients
        ]

        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    # Insert results
    for r in results:
        cur.execute("""
            INSERT INTO client_categories
            (scno, name, avg_consumption, variability,
             consumption_level, variability_level, final_category, calculated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (scno) DO UPDATE
            SET avg_consumption = EXCLUDED.avg_consumption,
                variability = EXCLUDED.variability,
                consumption_level = EXCLUDED.consumption_level,
                variability_level = EXCLUDED.variability_level,
                final_category = EXCLUDED.final_category,
                calculated_at = NOW();
        """, (
            r["scno"],
            r["name"],
            float(r["avg_consumption"]),
            float(r["variability"]),  # CV
            r["consumption_level"],
            r["variability_level"],
            r["final_category"]
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("\n✅ Finished! Categories updated.\n")


if __name__ == "__main__":
    main()
