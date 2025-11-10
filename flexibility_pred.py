import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading, warnings
warnings.filterwarnings("ignore")

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    "host": "localhost",
    "dbname": "elements_flex",
    "user": "postgres",
    "password": "ABcd1234!@",  # change if needed
    "port": 5432
}

CLIENTS_API = "https://ee.elementsenergies.com/api/fetchAllParUniqueMSN"
CONSUMPTION_API = "https://ee.elementsenergies.com/api/fetchHourlyConsumption?scno={}&date={}"
MAX_WORKERS = 10
lock = threading.Lock()

# ---------------- DB CONNECT ---------------- #
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# ---------------- FETCH CLIENTS ---------------- #
def fetch_clients():
    try:
        r = requests.get(CLIENTS_API, timeout=30)
        r.raise_for_status()
        data = r.json()
        clients = [(c["scno"], c["short_name"]) for c in data if "scno" in c and "short_name" in c]
        return clients
    except Exception as e:
        print("Error fetching clients:", e)
        return []

# ---------------- HELPER ---------------- #
def parse_hour_field(hour_value):
    if hour_value is None:
        raise ValueError("hour is None")
    hs = str(hour_value).strip().split(":")[0]
    hour_int = int(float(hs))
    if not (0 <= hour_int <= 23):
        raise ValueError(f"Parsed hour out of range: {hour_int}")
    return hour_int

# ---------------- FETCH CONSUMPTION ---------------- #
def fetch_consumption(scno, start_date, end_date):
    all_data = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        try:
            url = CONSUMPTION_API.format(scno, date_str)
            r = requests.get(url, timeout=10)
            if r.status_code == 404:
                current_date += timedelta(days=1)
                continue
            r.raise_for_status()
            daily = r.json()
            if isinstance(daily, list):
                for e in daily:
                    if "hour" in e and "consumption" in e:
                        try:
                            hour = parse_hour_field(e["hour"])
                            cons = float(e["consumption"])
                            all_data.append((scno, date_str, hour, cons))
                        except Exception:
                            continue
        except Exception:
            pass
        current_date += timedelta(days=1)
    return all_data

# ---------------- CALCULATE FLEXIBILITY ---------------- #
def calculate_flexibility(df):
    if df.empty:
        return None
    df["consumption"] = pd.to_numeric(df["consumption"], errors="coerce").fillna(0.0)

    # Load Factor (LF)
    daily_profiles = df.groupby(["date", "hour"])["consumption"].sum().reset_index()
    lf_list = [
        g["consumption"].mean() / g["consumption"].max()
        for _, g in daily_profiles.groupby("date") if g["consumption"].max() != 0
    ]
    LF = float(np.mean(lf_list)) if lf_list else None

    # Load Variability Index (LVI)
    daily_totals = df.groupby("date")["consumption"].sum()
    LVI = float(daily_totals.std() / daily_totals.mean()) if len(daily_totals) > 1 and daily_totals.mean() != 0 else None

    # Daily Load Shape Stability (DLSS)
    pivot = daily_profiles.pivot(index="hour", columns="date", values="consumption").fillna(0)
    DLSS = None
    if pivot.shape[1] >= 2:
        typical_day = pivot.mean(axis=1)
        correlations = [
            np.corrcoef(pivot[c], typical_day)[0, 1]
            for c in pivot.columns if not np.isnan(np.corrcoef(pivot[c], typical_day)[0, 1])
        ]
        DLSS = float(np.mean(correlations)) if correlations else None

    return LF, LVI, DLSS

# ---------------- RANK CLIENTS ---------------- #
def rank_clients(df):
    if df.empty:
        return df
    df = df.dropna(subset=["LF", "LVI", "DLSS"]).copy()

    df["LF_norm"] = 1 - (df["LF"] - df["LF"].min()) / (df["LF"].max() - df["LF"].min())
    df["LVI_norm"] = (df["LVI"] - df["LVI"].min()) / (df["LVI"].max() - df["LVI"].min())
    df["DLSS_norm"] = 1 - (df["DLSS"] - df["DLSS"].min()) / (df["DLSS"].max() - df["DLSS"].min())

    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["LF_norm", "LVI_norm", "DLSS_norm"])
    df["Flexibility_Index"] = df[["LF_norm", "LVI_norm", "DLSS_norm"]].mean(axis=1)
    df["Flexibility_Rank"] = df["Flexibility_Index"].rank(ascending=False, method="min").astype(int)
    return df.sort_values("Flexibility_Rank").reset_index(drop=True)

# ---------------- PROCESS CLIENT ---------------- #
def process_client(scno, name, start_date, end_date, conn):
    try:
        cur = conn.cursor()

        # ✅ Skip if already processed today
        cur.execute("SELECT calculated_at FROM flexibility_metrics WHERE scno=%s;", (scno,))
        row = cur.fetchone()
        if row and row[0] and row[0].date() == date.today():
            with lock:
                print(f"⏭ {name} ({scno}) — already processed today.")
            return None

        with lock:
            print(f"⚙️ Processing {name} ({scno})...")

        # --- Get last available date --- #
        cur.execute("SELECT MAX(date) FROM consumption WHERE scno=%s;", (scno,))
        last_date_row = cur.fetchone()
        last_date = last_date_row[0]

        fetch_end = datetime.today().date() - timedelta(days=1)
        if last_date:
            fetch_start = last_date + timedelta(days=1)
        else:
            fetch_start = start_date.date() if isinstance(start_date, datetime) else start_date

        # --- Fetch new data --- #
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
                print(f"✅ {name} ({scno}) — data updated until today.")
        else:
            with lock:
                print(f"⏩ {name} ({scno}) — already up to date.")

        # --- Calculate flexibility --- #
        df = pd.read_sql("SELECT date, hour, consumption FROM consumption WHERE scno=%s;", conn, params=(scno,))
        if df.empty:
            return None

        flex = calculate_flexibility(df)
        if flex:
            lf, lvi, dlss = flex
            return {"scno": scno, "name": name, "LF": lf, "LVI": lvi, "DLSS": dlss}

    except Exception as e:
        print(f"❌ Error {scno}: {e}")
    finally:
        conn.close()
    return None

# ---------------- MAIN ---------------- #
def main():
    conn = get_conn()
    cur = conn.cursor()

    # --- Fetch clients --- #
    clients = fetch_clients()
    if not clients:
        print("No clients fetched.")
        return

    # --- Check if all clients already processed today --- #
    cur.execute("SELECT COUNT(*) FROM clients;")
    total_clients = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM flexibility_metrics
        WHERE DATE(calculated_at) = CURRENT_DATE;
    """)
    processed_today = cur.fetchone()[0]

    if total_clients > 0 and processed_today >= total_clients:
        print("⏭ All clients already processed today.")
        print("✅ All done! Data updated until today.\n")
        cur.close()
        conn.close()
        return

    # --- Normal processing if not all done --- #
    end_date = datetime.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=60)

    execute_values(cur, """
        INSERT INTO clients (scno, short_name)
        VALUES %s
        ON CONFLICT (scno) DO UPDATE SET short_name = EXCLUDED.short_name;
    """, clients)
    conn.commit()

    print(f"\n🚀 Updating data for {len(clients)} clients...\n")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_client, scno, name, start_date, end_date, get_conn())
            for scno, name in clients
        ]
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    if results:
        df = pd.DataFrame(results)
        ranked = rank_clients(df)
        print("\n🏆 Ranking complete!\n")

        for _, row in ranked.iterrows():
            cur.execute("""
                INSERT INTO flexibility_metrics (scno, lf, lvi, dlss, flexibility_index, flexibility_rank, calculated_at)
                VALUES (%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (scno) DO UPDATE
                SET lf=EXCLUDED.lf, lvi=EXCLUDED.lvi, dlss=EXCLUDED.dlss,
                    flexibility_index=EXCLUDED.flexibility_index,
                    flexibility_rank=EXCLUDED.flexibility_rank,
                    calculated_at=NOW();
            """, (row["scno"], row["LF"], row["LVI"], row["DLSS"], row["Flexibility_Index"], row["Flexibility_Rank"]))
        conn.commit()

    cur.close()
    conn.close()
    print("\n✅ All done! Data updated until today.\n")

if __name__ == "__main__":
    main()
