import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType
import datetime
import os
import psycopg2

# Configuration Kafka
kafka_server = "kafka:9092"
input_topic = "coin"

# Configuration PostgreSQL
postgres_url = "jdbc:postgresql://192.168.1.17:5432/"
postgres_properties = {
    "user": "hive",
    "password": "hive",
    "driver": "org.postgresql.Driver"
}

# Configuration Spark
spark_version = '3.2.3'
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},org.postgresql:postgresql:42.2.23 streaming.py'

spark = SparkSession.builder.appName("KafkaStream").getOrCreate()
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

# Fonction pour créer la base de données et la table si elles n'existent pas
def create_db_and_table(db_name, table_name):
    conn = psycopg2.connect(host="192.168.1.17", port="5432", user="hive", password="hive", dbname="postgresql")
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(f"CREATE DATABASE {db_name}")

    conn.close()
    conn = psycopg2.connect(host="192.168.1.17", port="5432", user="hive", password="hive", dbname=db_name)
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            close_time TIMESTAMP,
            quote_asset_volume DOUBLE PRECISION,
            number_of_trades INT,
            taker_buy_base_asset_volume DOUBLE PRECISION,
            taker_buy_quote_asset_volume DOUBLE PRECISION,
            ignore TEXT,
            interval TEXT,
            coin TEXT,  -- Ajout de cette ligne
            year INT,
            month INT
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()

# Fonction optimisée pour écrire dans PostgreSQL sans itérer
def write_to_postgres(df_batch, batch_id):
    if df_batch.count() == 0:
        logging.info(f"Lot vide reçu. Aucune donnée à traiter pour le batch {batch_id}.")
        return  # Sortie de la fonction si le DataFrame est vide

    # Vérifier que la colonne "coin" existe
    if "coin" not in df_batch.columns:
        logging.error("Erreur : La colonne 'coin' est manquante dans le DataFrame.")
        return

    # Obtenir les informations de base de données et de table
    coin = df_batch.select("coin").distinct().first()['coin']
    interval = df_batch.select("interval").distinct().first()['interval']
    db_name = f"{coin.lower()}_db"
    table_name = f"table_{interval}"

    # Créer la base de données et la table si elles n'existent pas
    create_db_and_table(db_name, table_name)

    # Écrire tout le DataFrame dans PostgreSQL en mode "append"
    df_batch.write \
        .format("jdbc") \
        .option("url", f"{postgres_url}{db_name}") \
        .option("dbtable", table_name) \
        .option("user", postgres_properties["user"]) \
        .option("password", postgres_properties["password"]) \
        .option("driver", postgres_properties["driver"]) \
        .mode("append") \
        .save()

    logging.info(f"Traitement terminé pour le batch {batch_id}. Données insérées dans la table {db_name}.{table_name}.")

# Écrire les données dans PostgreSQL
query = df \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()
spark.stop()
