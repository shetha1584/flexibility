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
    "password": "ABcd1234!@",  # change if needed
    "port": 5432
}

IGNORE_SCNOS = {"ELR1115", "ELR1158"}

# Define peak hours (6–10 AM and 6–10 PM)
PEAK_HOURS = list(range(6, 10)) + list(range(18, 22))

# ---------------- DB CONNECT ---------------- #
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

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

    # Daily Load Shape Stability (DLSS) - Correlation based
    pivot = daily_profiles.pivot(index="hour", columns="date", values="consumption").fillna(0)
    DLSS = None
    if pivot.shape[1] >= 2:
        typical_day = pivot.mean(axis=1)
        correlations = [
            np.corrcoef(pivot[c], typical_day)[0, 1]
            for c in pivot.columns if not np.isnan(np.corrcoef(pivot[c], typical_day)[0, 1])
        ]
        DLSS = float(np.mean(correlations)) if correlations else None

    # Peak-hour usage ratio
    peak_usage = df[df["hour"].isin(PEAK_HOURS)]["consumption"].sum()
    total_usage = df["consumption"].sum()
    peak_ratio = float(peak_usage / total_usage) if total_usage > 0 else 0.0

    return LF, LVI, DLSS, peak_ratio

# ---------------- MAIN ---------------- #
def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT scno, short_name FROM clients;")
    clients = [(r[0], r[1]) for r in cur.fetchall() if r[0] not in IGNORE_SCNOS]
    print(f"\n🚀 Found {len(clients)} clients to process (ignored: {', '.join(IGNORE_SCNOS)}).\n")

    weekday_results, saturday_dlss, sunday_dlss = [], [], []

    for scno, name in clients:
        df = pd.read_sql("SELECT date, hour, consumption FROM consumption WHERE scno=%s;", conn, params=(scno,))
        if df.empty:
            print(f"⚠️ No data for {name} ({scno}), skipping.")
            continue

        df["date"] = pd.to_datetime(df["date"])
        df["day_name"] = df["date"].dt.day_name()

        df_weekday = df[df["day_name"].isin(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])]
        df_saturday = df[df["day_name"] == "Saturday"]
        df_sunday = df[df["day_name"] == "Sunday"]

        # Weekday full flexibility
        weekday_flex = calculate_flexibility(df_weekday)
        if weekday_flex:
            lf, lvi, dlss, peak_ratio = weekday_flex
            dlss_norm = (dlss + 1) / 2 if dlss is not None else None
            weekday_results.append({
                "scno": scno, "LF": lf, "LVI": lvi,
                "DLSS": dlss_norm, "Peak_Ratio": peak_ratio
            })

        # Saturday DLSS only
        if not df_saturday.empty:
            _, _, dlss_sat, _ = calculate_flexibility(df_saturday)
            if dlss_sat is not None:
                dlss_sat_norm = (dlss_sat + 1) / 2
                saturday_dlss.append({"scno": scno, "DLSS": dlss_sat_norm})

        # Sunday DLSS only
        if not df_sunday.empty:
            _, _, dlss_sun, _ = calculate_flexibility(df_sunday)
            if dlss_sun is not None:
                dlss_sun_norm = (dlss_sun + 1) / 2
                sunday_dlss.append({"scno": scno, "DLSS": dlss_sun_norm})

    # Store Weekday metrics
    for row in weekday_results:
        cur.execute("""
            INSERT INTO flexibility_metrics (
                scno, lf_weekday, lvi_weekday, dlss_weekday,
                peak_ratio_weekday, calculated_at
            )
            VALUES (%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (scno) DO UPDATE
            SET lf_weekday=EXCLUDED.lf_weekday,
                lvi_weekday=EXCLUDED.lvi_weekday,
                dlss_weekday=EXCLUDED.dlss_weekday,
                peak_ratio_weekday=EXCLUDED.peak_ratio_weekday,
                calculated_at=NOW();
        """, (
            row["scno"],
            row["LF"], row["LVI"], row["DLSS"], row["Peak_Ratio"]
        ))

    # Store Saturday DLSS
    for row in saturday_dlss:
        cur.execute("""
            INSERT INTO flexibility_metrics (scno, dlss_saturday, calculated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (scno) DO UPDATE
            SET dlss_saturday=EXCLUDED.dlss_saturday, calculated_at=NOW();
        """, (row["scno"], row["DLSS"]))

    # Store Sunday DLSS
    for row in sunday_dlss:
        cur.execute("""
            INSERT INTO flexibility_metrics (scno, dlss_sunday, calculated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (scno) DO UPDATE
            SET dlss_sunday=EXCLUDED.dlss_sunday, calculated_at=NOW();
        """, (row["scno"], row["DLSS"]))

    conn.commit()
    cur.close()
    conn.close()
    print("\n💾 Weekday metrics and normalized Saturday/Sunday DLSS stored successfully.")

if __name__ == "__main__":
    main()
