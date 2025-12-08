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

    # Peak-hour usage ratio
    peak_usage = df[df["hour"].isin(PEAK_HOURS)]["consumption"].sum()
    total_usage = df["consumption"].sum()
    peak_ratio = peak_usage / total_usage if total_usage > 0 else 0

    return LF, LVI, DLSS, peak_ratio

# ---------------- RANK CLIENTS ---------------- #
def rank_clients(df, period_label):
    if df.empty:
        return df

    df = df.dropna(subset=["LF", "LVI", "DLSS"]).copy()

    # Normalize
    df["LF_norm"] = 1 - (df["LF"] - df["LF"].min()) / (df["LF"].max() - df["LF"].min())
    df["LVI_norm"] = (df["LVI"] - df["LVI"].min()) / (df["LVI"].max() - df["LVI"].min())
    df["DLSS_norm"] = 1 - (df["DLSS"] - df["DLSS"].min()) / (df["DLSS"].max() - df["DLSS"].min())

    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["LF_norm", "LVI_norm", "DLSS_norm"])
    df["Flexibility_Index"] = df[["LF_norm", "LVI_norm", "DLSS_norm"]].mean(axis=1)

    # --- Penalize if already operating mostly off-peak ---
    df["Flexibility_Reason"] = "Normal"
    off_peak_mask = df["Peak_Ratio"] < 0.3
    df.loc[off_peak_mask, "Flexibility_Index"] *= 0.7
    df.loc[off_peak_mask, "Flexibility_Reason"] = "Less Flexible — already in off-peak"

    df["Flexibility_Rank"] = df["Flexibility_Index"].rank(ascending=False, method="min").astype(int)
    df["Period"] = period_label
    return df.sort_values("Flexibility_Rank").reset_index(drop=True)

# ---------------- MAIN ---------------- #
def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT scno, short_name FROM clients;")
    clients = [(r[0], r[1]) for r in cur.fetchall() if r[0] not in IGNORE_SCNOS]
    print(f"\n🚀 Found {len(clients)} clients to process (ignored: {', '.join(IGNORE_SCNOS)}).\n")

    weekday_results, weekend_results = [], []

    for scno, name in clients:
        df = pd.read_sql("SELECT date, hour, consumption FROM consumption WHERE scno=%s;", conn, params=(scno,))
        if df.empty:
            print(f"⚠️ No data for {name} ({scno}), skipping.")
            continue

        df["date"] = pd.to_datetime(df["date"])
        df["day_type"] = df["date"].dt.dayofweek.apply(lambda x: "Weekend" if x >= 5 else "Weekday")

        df_weekday = df[df["day_type"] == "Weekday"]
        df_weekend = df[df["day_type"] == "Weekend"]

        weekday_flex = calculate_flexibility(df_weekday)
        weekend_flex = calculate_flexibility(df_weekend)

        if weekday_flex:
            lf, lvi, dlss, peak_ratio = weekday_flex
            weekday_results.append({
                "scno": scno, "name": name, "LF": lf, "LVI": lvi,
                "DLSS": dlss, "Peak_Ratio": peak_ratio
            })

        if weekend_flex:
            lf, lvi, dlss, peak_ratio = weekend_flex
            weekend_results.append({
                "scno": scno, "name": name, "LF": lf, "LVI": lvi,
                "DLSS": dlss, "Peak_Ratio": peak_ratio
            })

    ranked_weekday = rank_clients(pd.DataFrame(weekday_results), "Weekday")
    ranked_weekend = rank_clients(pd.DataFrame(weekend_results), "Weekend")

    print("\n🏆 Weekday and Weekend Rankings calculated with off-peak adjustment.\n")

    # --- Store results --- #
    for _, row in ranked_weekday.iterrows():
        cur.execute("""
            INSERT INTO flexibility_metrics (
                scno, lf_weekday, lvi_weekday, dlss_weekday,
                peak_ratio_weekday, reason_weekday,
                flexibility_rank_weekday, calculated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (scno) DO UPDATE
            SET lf_weekday=EXCLUDED.lf_weekday,
                lvi_weekday=EXCLUDED.lvi_weekday,
                dlss_weekday=EXCLUDED.dlss_weekday,
                peak_ratio_weekday=EXCLUDED.peak_ratio_weekday,
                reason_weekday=EXCLUDED.reason_weekday,
                flexibility_rank_weekday=EXCLUDED.flexibility_rank_weekday,
                calculated_at=NOW();
        """, (
            row["scno"], row["LF"], row["LVI"], row["DLSS"],
            row["Peak_Ratio"], row["Flexibility_Reason"], row["Flexibility_Rank"]
        ))

    for _, row in ranked_weekend.iterrows():
        cur.execute("""
            INSERT INTO flexibility_metrics (
                scno, lf_weekend, lvi_weekend, dlss_weekend,
                peak_ratio_weekend, reason_weekend,
                flexibility_rank_weekend, calculated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (scno) DO UPDATE
            SET lf_weekend=EXCLUDED.lf_weekend,
                lvi_weekend=EXCLUDED.lvi_weekend,
                dlss_weekend=EXCLUDED.dlss_weekend,
                peak_ratio_weekend=EXCLUDED.peak_ratio_weekend,
                reason_weekend=EXCLUDED.reason_weekend,
                flexibility_rank_weekend=EXCLUDED.flexibility_rank_weekend,
                calculated_at=NOW();
        """, (
            row["scno"], row["LF"], row["LVI"], row["DLSS"],
            row["Peak_Ratio"], row["Flexibility_Reason"], row["Flexibility_Rank"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("\n✅ Rankings stored successfully, including off-peak reason!")

if __name__ == "__main__":
    main()
