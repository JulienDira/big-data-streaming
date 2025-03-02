import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, IntegerType, StringType
import os

# Configuration de Spark
spark = SparkSession.builder \
    .appName("PostgresToHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.1.17:9000") \
    .config("spark.jars", "/path/to/postgresql-42.5.4.jar") \  # Chemin vers le driver JDBC PostgreSQL
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Configuration PostgreSQL
postgres_url_template = "jdbc:postgresql://192.168.1.17:5432/{}"  # Template pour les URLs des bases de données
postgres_properties = {
    "user": "hive",
    "password": "hive",
    "driver": "org.postgresql.Driver"
}

# Configuration HDFS
hdfs_directory = "hdfs://192.168.1.17:9000/RAW/longtime/"

# Liste des bases de données et des tables
databases = ["source_btcusdc_db", "source_ethusdc_db", "source_solusdc_db", "source_xrpusdc_db"]
tables = ["table_1m", "table_5m", "table_15m", "table_1h", "table_1d"]

# Schéma des données (identique pour toutes les tables)
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("close_time", TimestampType(), True),
    StructField("quote_asset_volume", DoubleType(), True),
    StructField("number_of_trades", IntegerType(), True),
    StructField("taker_buy_base_asset_volume", DoubleType(), True),
    StructField("taker_buy_quote_asset_volume", DoubleType(), True),
    StructField("ignore", StringType(), True),
    StructField("interval", StringType(), True),
    StructField("coin", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True)
])

# Fonction pour lire les données initiales depuis une table
def read_initial_data(database, table):
    postgres_url = postgres_url_template.format(database)
    df = spark.read \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", table) \
        .option("user", postgres_properties["user"]) \
        .option("password", postgres_properties["password"]) \
        .option("driver", postgres_properties["driver"]) \
        .schema(schema) \
        .load()
    return df

# Fonction pour écrire les données initiales dans HDFS
def write_initial_data(df, database, table):
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", f"{hdfs_directory}/{database}/{table}/initial_load") \
        .save()

# Fonction pour lire les nouvelles données en streaming depuis une table
def read_streaming_data(database, table):
    postgres_url = postgres_url_template.format(database)
    df = spark.readStream \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", f"(SELECT * FROM {table} WHERE timestamp > NOW() - INTERVAL '1 hour') AS tmp") \
        .option("user", postgres_properties["user"]) \
        .option("password", postgres_properties["password"]) \
        .option("driver", postgres_properties["driver"]) \
        .schema(schema) \  # Appliquer le schéma complet
        .option("fetchsize", 1000) \
        .load()
    return df

# Fonction pour écrire les nouvelles données en streaming dans HDFS
def write_streaming_data(df, database, table):
    query = df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", f"/tmp/spark_checkpoint/{database}/{table}") \
        .option("path", f"{hdfs_directory}/{database}/{table}/streaming_data") \
        .outputMode("append") \
        .start()
    return query

# Traitement des données initiales
for database in databases:
    for table in tables:
        logging.info(f"Traitement initial de la table {table} dans la base de données {database}...")
        df_initial = read_initial_data(database, table)
        write_initial_data(df_initial, database, table)
        logging.info(f"Données initiales de la table {table} dans la base de données {database} écrites dans HDFS.")

# Traitement des données en streaming
queries = []
for database in databases:
    for table in tables:
        logging.info(f"Démarrage du streaming pour la table {table} dans la base de données {database}...")
        df_streaming = read_streaming_data(database, table)
        query = write_streaming_data(df_streaming, database, table)
        queries.append(query)

# Attendre la fin du streaming
for query in queries:
    query.awaitTermination()