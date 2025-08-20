
# https://chatgpt.com/c/68613c87-d62c-8002-9e77-4f644e3300cb

# pip install faker pyarrow
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1 --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.spark_catalog.type=hive --conf spark.sql.catalog.spark_catalog.uri=thrift://hive:9083 --conf spark.sql.catalogImplementation=hive
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1 --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.spark_catalog.type=hive --conf spark.sql.catalog.spark_catalog.uri=thrift://hive:9083 --conf spark.sql.catalogImplementation=hive

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType

# 1. Spark + Iceberg Session Setup
spark = SparkSession.builder \
    .appName("KafkaToIcebergStreamingLLM") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "4")
# 2. Kafka Message Schema (JSON)
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", FloatType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("transaction_time", TimestampType()),
    StructField("location", StringType())
])

# 3. Read Kafka Stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse Kafka value (assumes JSON string)
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .withColumn("json", from_json(col("value"), schema)) \
    .select("json.*")

# 5. Write Streaming Data to Iceberg Transactions Table
query = parsed_df.writeStream \
    .format("iceberg") \
    .option("checkpointLocation", "/tmp/iceberg-checkpoint") \
    .outputMode("append") \
    .toTable("spark_catalog.default.transactions")

# 6. Create Customer Summary Table if not exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS spark_catalog.default.customer_summary (
        customer_id STRING,
        total_spent DOUBLE,
        num_transactions INT,
        summary STRING
    )
    USING iceberg
""")

# 7. Define LLM Summary Generator UDF
@pandas_udf(StringType())
def generate_summary_udf(customer_id: pd.Series, total_spent: pd.Series, num_transactions: pd.Series) -> pd.Series:
    summaries = []
    for cid, spent, txns in zip(customer_id, total_spent, num_transactions):
        prompt = f"""
        Generate a customer spending summary:
        - Customer ID: {cid}
        - Total Spent: ${spent:.2f}
        - Number of Transactions: {txns}
        Use a friendly, professional tone.
        """
        try:
            response = requests.post(
                "http://host.docker.internal:11434/api/generate",
                json={
                    "model": "llama3.2:1b",# "llama3:latest",
                    "prompt": prompt,
                    "stream": False,
                    "temperature": 0.3,
                    "top_k": 20,
                    "top_p": 0.8
                },
                timeout=30
            )
            summaries.append(response.json().get("response", ""))
        except Exception as e:
            summaries.append(f"Error: {str(e)}")
    return pd.Series(summaries)


# 8. Periodic Batch Job to Read Iceberg and Generate Summaries
def process_batch(batch_df, batch_id):
    agg_df = batch_df.groupBy("customer_id") \
        .agg({"amount": "sum", "transaction_id": "count"}) \
        .withColumnRenamed("sum(amount)", "total_spent") \
        .withColumnRenamed("count(transaction_id)", "num_transactions")
    summary_df = agg_df.withColumn(
        "summary",
        generate_summary_udf("customer_id", "total_spent", "num_transactions")
    )
    summary_df.writeTo("spark_catalog.default.customer_summary").overwritePartitions()


parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/summary-checkpoint") \
    .trigger(processingTime="1 minute") \
    .start() \
    .awaitTermination()
