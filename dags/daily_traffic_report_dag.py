from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


PROCESSED_DATA_PATH = Path("/opt/airflow/data/processed/traffic_windows")
REPORTS_DIR = Path("/opt/airflow/reports")
REPORT_FILE = REPORTS_DIR / "daily_traffic_report.csv"
REPORT_COLUMNS = [
    "junction_name",
    "hour_of_day",
    "peak_vehicle_count",
    "average_speed",
    "average_congestion_index",
    "recommendation",
]


def build_daily_report():
    """Read processed parquet files and generate a simple daily traffic report."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    if not PROCESSED_DATA_PATH.exists():
        print("[Airflow] No processed traffic data found yet.")
        empty_df = pd.DataFrame(columns=REPORT_COLUMNS)
        empty_df.to_csv(REPORT_FILE, index=False)
        print(f"[Airflow] Empty report created at {REPORT_FILE}")
        return

    parquet_files = list(PROCESSED_DATA_PATH.rglob("*.parquet"))
    if not parquet_files:
        print("[Airflow] Parquet folder exists but no files are available yet.")
        pd.DataFrame(columns=REPORT_COLUMNS).to_csv(REPORT_FILE, index=False)
        print(f"[Airflow] Empty report created at {REPORT_FILE}")
        return

    print("[Airflow] Reading processed traffic data from Parquet...")
    traffic_df = pd.read_parquet(PROCESSED_DATA_PATH)

    if traffic_df.empty:
        print("[Airflow] Processed data is empty.")
        pd.DataFrame(columns=REPORT_COLUMNS).to_csv(REPORT_FILE, index=False)
        return

    traffic_df["window_start"] = pd.to_datetime(traffic_df["window_start"])
    traffic_df["hour_of_day"] = traffic_df["window_start"].dt.hour

    print("[Airflow] Aggregating traffic by junction and hour...")
    hourly_df = (
        traffic_df.groupby(["junction_name", "hour_of_day"], as_index=False)
        .agg(
            peak_vehicle_count=("total_vehicle_count", "sum"),
            average_speed=("avg_speed", "mean"),
            average_congestion_index=("avg_congestion_index", "mean"),
        )
    )

    print("[Airflow] Finding peak traffic hour for each junction...")
    peak_df = (
        hourly_df.sort_values(["junction_name", "peak_vehicle_count"], ascending=[True, False])
        .groupby("junction_name", as_index=False)
        .first()
    )

    def recommendation(row):
        if row["average_speed"] < 15 or row["average_congestion_index"] > 8:
            return "Traffic police intervention needed"
        return "Normal monitoring is enough"

    peak_df["recommendation"] = peak_df.apply(recommendation, axis=1)
    peak_df.to_csv(REPORT_FILE, index=False)
    print(f"[Airflow] Daily report generated successfully at {REPORT_FILE}")


with DAG(
    dag_id="daily_traffic_report_dag",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["traffic", "colombo", "batch"],
) as dag:
    generate_report = PythonOperator(
        task_id="generate_daily_traffic_report",
        python_callable=build_daily_report,
    )

    generate_report
