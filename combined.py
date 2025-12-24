import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import warnings
warnings.filterwarnings("ignore")

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    "host": "localhost",
    "dbname": "elements_flex",
    "user": "postgres",
    "password": "ABcd1234!@",
    "port": 5432
}

IGNORE_SCNOS = {"ELR1115", "ELR1158"}


# ---------------- DB CONNECT ---------------- #
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------------- CALCULATE ONLY DLSS ---------------- #
def calculate_dlss(df):
    if df.empty:
        return None

    df["consumption"] = pd.to_numeric(df["consumption"], errors="coerce").fillna(0.0)

    daily_profiles = df.groupby(["date", "hour"])["consumption"].sum().reset_index()
    pivot = daily_profiles.pivot(index="hour", columns="date", values="consumption").fillna(0)

    if pivot.shape[1] < 2:
        return None

    typical_day = pivot.mean(axis=1)

    correlations = [
        np.corrcoef(pivot[c], typical_day)[0, 1]
        for c in pivot.columns
        if not np.isnan(np.corrcoef(pivot[c], typical_day)[0, 1])
    ]

    if not correlations:
        return None

    dlss = float(np.mean(correlations))
    return (dlss + 1) / 2


# ---------------- MAIN ---------------- #
def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT scno, short_name FROM clients;")
    clients = [(r[0], r[1]) for r in cur.fetchall() if r[0] not in IGNORE_SCNOS]

    print(f"\n🚀 Found {len(clients)} clients to process (ignored: {', '.join(IGNORE_SCNOS)}).\n")

    weekday_dlss = []

    for scno, name in clients:
        df = pd.read_sql("SELECT date, hour, consumption FROM consumption WHERE scno=%s;",
                         conn, params=(scno,))

        if df.empty:
            print(f"⚠️ No data for {name} ({scno}), skipping.")
            continue

        df["date"] = pd.to_datetime(df["date"])
        df["day_name"] = df["date"].dt.day_name()

        df_weekday = df[df["day_name"].isin(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])]
        df_saturday = df[df["day_name"] == "Saturday"]
        df_sunday = df[df["day_name"] == "Sunday"]

        dlss_wd = calculate_dlss(df_weekday)
        dlss_sat = calculate_dlss(df_saturday)
        dlss_sun = calculate_dlss(df_sunday)

        weekday_dlss.append((scno, dlss_wd, dlss_sat, dlss_sun))

    # ---------------- SAVE TO NEW TABLE ---------------- #
    insert_sql = """
        INSERT INTO dlss_results (scno, dlss_weekday, dlss_saturday, dlss_sunday)
        VALUES %s
        ON CONFLICT (scno) DO UPDATE
        SET 
            dlss_weekday  = EXCLUDED.dlss_weekday,
            dlss_saturday = EXCLUDED.dlss_saturday,
            dlss_sunday   = EXCLUDED.dlss_sunday,
            calculated_at = NOW()
    """

    execute_values(cur, insert_sql, weekday_dlss)
    conn.commit()

    cur.close()
    conn.close()

    print("\n💾 DLSS values stored successfully in dlss_results table.")


if __name__ == "__main__":
    main()
