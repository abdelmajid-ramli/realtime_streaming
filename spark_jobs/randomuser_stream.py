from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# -----------------------------
# 1. SPARK SESSION
# -----------------------------
spark = (
    SparkSession.builder
    .appName("RandomUserKafkaStream")
    .master("spark://172.26.0.4:7077")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# 2. SCHEMA DES DONNÉES RANDOMUSER
# -----------------------------
schema = StructType([
    StructField("gender", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("nat", StringType()),
])

# -----------------------------
# 3. LIRE DEPUIS KAFKA
# -----------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka_new:9092")
    .option("subscribe", "randomuser_results")
    .load()
)

# Kafka → JSON string
df2 = df.selectExpr("CAST(value AS STRING) as json")

# Parse du JSON
parsed = df2.select(from_json(col("json"), schema).alias("user"))

flattened = parsed.select(
    col("user.gender"),
    col("user.email"),
    col("user.phone"),
    col("user.nat")
)

# -----------------------------
# 4. WRITE STREAM → CONSOLE
# -----------------------------
query = (
    flattened.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
