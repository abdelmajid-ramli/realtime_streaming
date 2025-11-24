from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,lit,udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


from pyspark.sql.functions import concat_ws, sha2, col


# -----------------------------
# 1. SPARK SESSION
# -----------------------------
spark = (
    SparkSession.builder
    .appName("RandomUserKafkaStream")
    .master("spark://172.26.0.4:7077")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.cassandra.connection.host", "cassandra_new") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# 2. SCHEMA DES DONNÉES RANDOMUSER
# -----------------------------
""" schema = StructType([
    StructField("gender", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("nat", StringType()),
]) """

schema = StructType([
    StructField("gender", StringType(), True),
    StructField("name", StructType([
        StructField("title", StringType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True),
    ]), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ]), True),
    StructField("email", StringType(), True),
    StructField("dob", StructType([
        StructField("date", StringType(), True),
        StructField("age", IntegerType(), True)
    ]), True)
])

# -----------------------------
# 3. LIRE DEPUIS KAFKA
# -----------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka_new:9092")
    .option("subscribe", "randomuser_results")
    .option("startingOffsets", "earliest") 
    .load()
)

# Kafka → JSON string
df2 = df.selectExpr("CAST(value AS STRING) as json")

# Parse du JSON
parsed = df2.select(from_json(col("json"), schema).alias("user"))

""" flattened = parsed.select(
    col("user.gender"),
    col("user.email"),
    col("user.phone"),
    col("user.nat")
) """

flattened = parsed.select(
    col("user.gender"),
    col("user.name.title").alias("name_title"),
    col("user.name.first").alias("first_name"),
    col("user.name.last").alias("last_name"),
    col("user.location.city").alias("city"),
    col("user.location.country").alias("country"),
    col("user.email"),
    col("user.dob.date").alias("dob_date"),
    col("user.dob.age").alias("dob_age")
)

# -----------------------------
# 4. WRITE STREAM → CONSOLE
# -----------------------------


#flattened = flattened.withColumn("user_id", lit(str(py_uuid.uuid4())))


import uuid

# UDF pour générer un UUID
def make_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(make_uuid, StringType())

flattened = flattened.withColumn("user_id", uuid_udf())
flattened.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .outputMode("append") \
    .options(keyspace="randomuser", table="users") \
    .option("checkpointLocation", "/opt/checkpoints/randomuser") \
    .start() \
    .awaitTermination()
