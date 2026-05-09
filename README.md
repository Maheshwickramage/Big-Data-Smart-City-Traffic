# Smart City Traffic & Congestion System for Colombo

This mini project demonstrates a beginner-friendly Lambda/Kappa-style traffic pipeline for an Applied Big Data Engineering module.

The pipeline includes:

- A Python producer that simulates traffic sensors in Colombo
- Apache Kafka for real-time ingestion
- Spark Structured Streaming for stream processing
- Parquet for processed data storage
- Apache Airflow for hourly batch reporting
- Docker Compose to run the platform

## Project Architecture

1. `traffic_producer.py` generates JSON traffic events for Borella, Rajagiriya, Pettah, and Nugegoda.
2. Kafka stores incoming events in the `traffic-data` topic.
3. Spark Structured Streaming reads the topic, calculates congestion metrics, and creates 5-minute window results.
4. Normal processed data is written to Parquet in the `data/processed/traffic_windows/` folder.
5. Critical traffic events where `avg_speed < 10` are written to Kafka topic `critical-traffic`.
6. Airflow runs a scheduled batch job to generate `daily_traffic_report.csv`.

## Folder Structure

```text
Big_Data_Mini_Project/
├── producers/
│   └── traffic_producer.py
├── spark_jobs/
│   └── traffic_streaming.py
├── dags/
│   └── daily_traffic_report_dag.py
├── reports/
│   ├── .gitkeep
│   └── daily_traffic_report_sample.csv
├── data/
│   ├── .gitkeep
│   └── sample_messages.jsonl
├── docker-compose.yml
├── README.md
└── requirements.txt
```

## Traffic Message Format

Each message sent to Kafka topic `traffic-data` contains:

```json
{
  "sensor_id": "COL-001",
  "junction_name": "Borella",
  "event_time": "2026-05-09T14:00:00+00:00",
  "vehicle_count": 88,
  "avg_speed": 24.5
}
```

## Sample Output Data

Sample input events are stored in `data/sample_messages.jsonl`.

Sample batch report:

```csv
junction_name,hour_of_day,peak_vehicle_count,average_speed,average_congestion_index,recommendation
Borella,14,420,21.6,4.38,Normal monitoring is enough
Nugegoda,14,505,13.4,9.11,Traffic police intervention needed
Pettah,14,468,18.7,6.42,Normal monitoring is enough
Rajagiriya,14,544,9.8,12.85,Traffic police intervention needed
```

## Prerequisites

- Docker Desktop installed and running
- Docker Compose available

## How to Run the Project

### 1. Start all containers

```bash
docker compose up -d
```

### 2. Check running containers

```bash
docker compose ps
```

### 3. Run the traffic producer

```bash
docker compose exec producer python /opt/project/producers/traffic_producer.py
```

If you want to run the producer from your local machine instead of Docker:

```bash
pip install -r requirements.txt
KAFKA_BROKER=localhost:9092 python producers/traffic_producer.py
```

### 4. Start the Spark Structured Streaming job

Run this inside the Spark master container:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/project/spark_jobs/traffic_streaming.py
```

### 5. Open the Airflow UI

Airflow URL:

```text
http://localhost:8088
```

Login details:

```text
Username: admin
Password: admin
```

Trigger the DAG manually from the UI or wait for the hourly schedule.

### 6. Check generated report

```bash
cat reports/daily_traffic_report.csv
```

### 7. Read critical traffic alerts from Kafka

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic critical-traffic \
  --from-beginning
```

## Important Output Locations

- Processed normal traffic data: `data/processed/traffic_windows/`
- Spark checkpoints: `data/checkpoints/`
- Daily report: `reports/daily_traffic_report.csv`
- Sample report: `reports/daily_traffic_report_sample.csv`

## Beginner-Friendly Explanation

### Producer

The producer creates random traffic events every 2 seconds for the four junctions. It also occasionally creates critical traffic situations by setting `avg_speed` below `10 km/h`.

### Spark Streaming Job

The Spark job:

- Reads live traffic data from Kafka
- Converts JSON into columns
- Uses `event_time` as the event timestamp
- Creates a 5-minute tumbling window
- Calculates `congestion_index = vehicle_count / avg_speed`
- Writes normal windowed data to Parquet
- Sends critical alerts to Kafka topic `critical-traffic`

### Airflow Batch Job

The Airflow DAG:

- Reads processed Parquet files
- Aggregates traffic by junction and hour
- Finds the peak traffic hour for each junction
- Adds a recommendation column
- Saves the final CSV report

## Simple Demonstration Flow for Submission Video

1. Show the project folder structure.
2. Start Docker Compose with `docker compose up -d`.
3. Show Kafka, Spark, and Airflow containers using `docker compose ps`.
4. Run the producer and explain that it is generating Colombo traffic sensor data.
5. Run the Spark job and explain the 5-minute window and congestion calculation.
6. Open a Kafka consumer on `critical-traffic` and show alert messages when speed goes below `10 km/h`.
7. Open the Airflow UI and trigger `daily_traffic_report_dag`.
8. Open `reports/daily_traffic_report.csv` and explain the peak hour and recommendation column.
9. End by summarizing how the architecture supports real-time monitoring and batch reporting.

## Clean Logs You Can Mention in the Demo

- Producer logs show whether each event is `NORMAL` or `CRITICAL`
- Spark logs show when the streaming query starts
- Airflow logs clearly say when the report was generated

## Optional Cleanup Commands

Stop containers:

```bash
docker compose down
```

Stop containers and remove generated volumes:

```bash
docker compose down -v
```
