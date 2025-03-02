import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType
import datetime
import os

# Configuration Kafka
kafka_server = "kafka:9092"
input_topic = "shorttime"

# Configuration HDFS
hdfs_path = "hdfs://192.168.1.17:9000/RAW/shorttime/"

# Configuration Spark
spark_version = '3.2.3'
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},org.apache.spark:spark-avro_2.12:{spark_version} streaming-shorttime.py'

# Création de la session Spark avec la configuration HDFS
spark = SparkSession.builder \
    .appName("KafkaStream") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.1.17:9000") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Schéma des données
schema = StructType([ 
    StructField("coin", StringType(), True),
    StructField("timestamp", LongType(), True),  # timestamp en millisecondes
    StructField("open", DoubleType(), True),     # type numérique
    StructField("high", DoubleType(), True),     # type numérique
    StructField("low", DoubleType(), True),      # type numérique
    StructField("close", DoubleType(), True),    # type numérique
    StructField("volume", DoubleType(), True),   # type numérique
    StructField("close_time", LongType(), True), # timestamp en millisecondes
    StructField("quote_asset_volume", DoubleType(), True),    # type numérique
    StructField("number_of_trades", IntegerType(), True),
    StructField("taker_buy_base_asset_volume", DoubleType(), True),   # type numérique
    StructField("taker_buy_quote_asset_volume", DoubleType(), True),  # type numérique
    StructField("ignore", StringType(), True),
    StructField("interval", StringType(), True)
])

# Lire les données depuis Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Convertir le timestamp en date
df = df.withColumn("timestamp", (col("timestamp") / 1000).cast(TimestampType()))
df = df.withColumn("close_time", (col("close_time") / 1000).cast(TimestampType()))
df = df.withColumn("year", year(col("timestamp")))
df = df.withColumn("month", month(col("timestamp")))

# Configurez le logging pour l'application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Fonction pour écrire les données en Avro dans HDFS
def write_to_hdfs(df_batch, batch_id):
    if df_batch.count() == 0:
        logging.info(f"Lot vide reçu. Aucune donnée à traiter pour le batch {batch_id}.")
        return  # Sortie de la fonction si le DataFrame est vide

    # Écrire les données dans HDFS au format Avro
    df_batch.write \
        .format("avro") \
        .mode("append") \
        .option("path", f"{hdfs_path}coin_shorttime_batch_{batch_id}") \
        .option("dfs.replication", "2") \
        .save()

    logging.info(f"Traitement terminé pour le batch {batch_id}. Données écrites dans HDFS.")

# Écrire les données dans HDFS
query = df \
    .writeStream \
    .foreachBatch(write_to_hdfs) \
    .outputMode("update") \
    .start()

query.awaitTermination()
spark.stop()
