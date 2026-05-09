from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    sum as spark_sum,
    to_timestamp,
    to_json,
    struct,
    when,
    window,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
INPUT_TOPIC = "traffic-data"
CRITICAL_TOPIC = "critical-traffic"
NORMAL_DATA_PATH = "/opt/project/data/processed/traffic_windows"
NORMAL_CHECKPOINT_PATH = "/opt/project/data/checkpoints/traffic_windows"
CRITICAL_CHECKPOINT_PATH = "/opt/project/data/checkpoints/critical_traffic"


def create_spark_session():
    """Create a Spark session for Structured Streaming."""
    spark = (
        SparkSession.builder.appName("ColomboTrafficStreaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = create_spark_session()

    schema = StructType(
        [
            StructField("sensor_id", StringType(), True),
            StructField("junction_name", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("vehicle_count", IntegerType(), True),
            StructField("avg_speed", DoubleType(), True),
        ]
    )

    print("[Spark] Reading traffic data from Kafka...")
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .filter(col("event_time").isNotNull())
        .withColumn("congestion_index", col("vehicle_count") / col("avg_speed"))
        .withColumn("is_critical", when(col("avg_speed") < 10, True).otherwise(False))
    )

    print("[Spark] Creating 5-minute tumbling window aggregations...")
    normal_windowed_df = (
        parsed_df.filter(col("is_critical") == False)
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("sensor_id"),
            col("junction_name"),
        )
        .agg(
            spark_sum("vehicle_count").alias("total_vehicle_count"),
            avg("avg_speed").alias("avg_speed"),
            avg("congestion_index").alias("avg_congestion_index"),
        )
        .select(
            col("sensor_id"),
            col("junction_name"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_vehicle_count"),
            col("avg_speed"),
            col("avg_congestion_index"),
        )
    )

    critical_alerts_df = (
        parsed_df.filter(col("is_critical") == True)
        .select(
            col("sensor_id"),
            col("junction_name"),
            col("event_time"),
            col("vehicle_count"),
            col("avg_speed"),
            col("congestion_index"),
        )
    )

    print("[Spark] Writing normal windowed traffic data to Parquet...")
    normal_query = (
        normal_windowed_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", NORMAL_DATA_PATH)
        .option("checkpointLocation", NORMAL_CHECKPOINT_PATH)
        .start()
    )

    print("[Spark] Writing critical traffic alerts to Kafka topic 'critical-traffic'...")
    critical_query = (
        critical_alerts_df.select(to_json(struct("*")).alias("value"))
        .writeStream.outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", CRITICAL_TOPIC)
        .option("checkpointLocation", CRITICAL_CHECKPOINT_PATH)
        .start()
    )

    print("[Spark] Streaming job started successfully.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
