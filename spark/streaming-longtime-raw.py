import time
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, IntegerType, StringType

# Configuration de Spark
spark = SparkSession.builder \
    .appName("PostgresToHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.1.17:9000") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')  # Mode INFO pour moins de verbosité

# Configuration PostgreSQL
postgres_url_template = "jdbc:postgresql://192.168.1.17:5432/{}"
postgres_properties = {
    "user": "hive",
    "password": "hive",
    "driver": "org.postgresql.Driver"
}

# Configuration HDFS
hdfs_directory = "hdfs://192.168.1.17:9000/RAW/longtime/"

# Bases et tables
databases = ["source_btcusdc_db", "source_ethusdc_db", "source_solusdc_db", "source_xrpusdc_db"]
tables = ["table_1m", "table_5m", "table_15m", "table_1h", "table_1d"]

# Schéma des données
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

# Dictionnaire pour stocker les derniers timestamps par db et table
timestamps_dict = {}

# Charger les timestamps depuis un fichier
def load_timestamps_from_file():
    try:
        with open("timestamps.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

# Sauvegarder les timestamps dans un fichier
def save_timestamps_to_file():
    with open("timestamps.json", "w") as f:
        json.dump(timestamps_dict, f)

# Charger les timestamps au démarrage
timestamps_dict = load_timestamps_from_file()

# Sauvegarder les timestamps avant de quitter
import atexit
atexit.register(save_timestamps_to_file)

def get_last_timestamp(database, table):
    """
    Récupère le dernier timestamp pour une base de données et une table données depuis le dictionnaire.
    """
    if database in timestamps_dict and table in timestamps_dict[database]:
        last_timestamp_str = timestamps_dict[database][table]
        if last_timestamp_str:
            last_timestamp = datetime.strptime(last_timestamp_str, '%Y-%m-%d %H:%M:%S')
            print(f"Dernier timestamp trouvé pour {database}.{table} : {last_timestamp}")
            return last_timestamp
        else:
            print(f"Aucun timestamp trouvé pour {database}.{table}")
            return None
    else:
        print(f"Aucun timestamp trouvé pour {database}.{table} dans le dictionnaire")
        return None

def save_last_timestamp(database, table, timestamp):
    """
    Sauvegarde le dernier timestamp traité dans le dictionnaire pour une base de données et une table données.
    """
    if database not in timestamps_dict:
        timestamps_dict[database] = {}
    timestamps_dict[database][table] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Timestamp {timestamp} sauvegardé pour {database}.{table}")

def read_initial_data(database, table):
    """
    Lecture initiale complète des données depuis PostgreSQL.
    """
    postgres_url = postgres_url_template.format(database)
    return spark.read \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", table) \
        .option("user", postgres_properties["user"]) \
        .option("password", postgres_properties["password"]) \
        .option("driver", postgres_properties["driver"]) \
        .schema(schema) \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .load()

def write_initial_data(df, database, table):
    """
    Écriture initiale des données dans HDFS.
    """
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", f"{hdfs_directory}/{database}/{table}/initial_load") \
        .save()

def read_incremental_data(database, table):
    """
    Lecture des données incrémentales depuis PostgreSQL.
    """
    postgres_url = postgres_url_template.format(database)
    last_timestamp = get_last_timestamp(database, table)
    if not last_timestamp:
        print(f"Aucun timestamp trouvé pour {database} - {table}")
        query = f"(SELECT * FROM {table} WHERE timestamp > NOW() - INTERVAL '1 minute') AS tmp"
    else:
        last_timestamp_str = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        query = f"(SELECT * FROM {table} WHERE timestamp > '{last_timestamp_str}') AS tmp"
    
    return spark.read \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", query) \
        .option("user", postgres_properties["user"]) \
        .option("password", postgres_properties["password"]) \
        .option("driver", postgres_properties["driver"]) \
        .schema(schema) \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .load()

def write_incremental_data(df, database, table):
    """
    Écriture des données incrémentales dans HDFS.
    """
    print(f"Écriture des données pour {database} - {table}")
    if df.count() > 0:
        print("Des données ont été trouvées. Écriture dans HDFS.")
        max_timestamp = df.select(spark_max(col("timestamp"))).collect()[0][0]
        if max_timestamp:
            save_last_timestamp(database, table, max_timestamp)
        df.write \
            .format("parquet") \
            .mode("append") \
            .option("path", f"{hdfs_directory}/{database}/{table}/incremental_data") \
            .save()
    else:
        print(f"Aucune donnée trouvée à écrire pour {database} - {table}")

def process_initial_data(database, table):
    """
    Traitement initial des données pour une base de données et une table données.
    """
    df_initial = read_initial_data(database, table)
    write_initial_data(df_initial, database, table)
    if df_initial.count() > 0:
        max_timestamp = df_initial.select(spark_max(col("timestamp"))).collect()[0][0]
        print(f"!!!!!!!!!!!!!!!!!!!!{max_timestamp}!!!!!!!!!!!!!!!!!!!!!!!!")
        if max_timestamp:
            save_last_timestamp(database, table, max_timestamp)

def process_all_incremental_data():
    """
    Traitement incrémental des données pour toutes les bases de données et tables.
    """
    while True:
        for database in databases:
            for table in tables:
                print(f"Traitement des données incrémentales pour {database} - {table}")
                df_incremental = read_incremental_data(database, table)
                if df_incremental.count() > 0:
                    print(f"Écriture des données pour {database} - {table}")
                    write_incremental_data(df_incremental, database, table)
                else:
                    print(f"Aucune donnée trouvée pour {database} - {table}")
        time.sleep(60)  # Attendre 1 minute avant le prochain batch

# Chargement initial pour chaque base et table
for database in databases:
    for table in tables:
        process_initial_data(database, table)

# Lancer le traitement incrémental pour toutes les bases et tables
process_all_incremental_data()