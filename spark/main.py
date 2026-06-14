# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("velib-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN") # Je limite les log dans le cas ou c'est un WARNING ou une erreur


# --- BLOC : Source Kafk, connexion au topic

# kafka:29092 : adresse interne Docker du broker Kafka, car Spark tourne dans un container Docker séparé

raw_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "velib-stations").option("startingOffsets", "latest").load()


# --- BLOC : Schema JSON 

# le schéma est défini pour transformer les données brutes en données structurées (contrat de données entre producer et consumer)
# ce qui évite les erreurs de parsing et assure la qualité des données

schema = StructType([
    StructField("stationcode", StringType()),
    StructField("name", StringType()),
    StructField("is_installed", StringType()),
    StructField("is_renting", StringType()),
    StructField("is_returning", StringType()),
    StructField("capacity", IntegerType()),
    StructField("numdocksavailable", IntegerType()),
    StructField("numbikesavailable", IntegerType()),
    StructField("mechanical", IntegerType()),
    StructField("ebike", IntegerType()),
    StructField("duedate", TimestampType()),
    StructField("nom_arrondissement_communes", StringType()),
    StructField("code_insee_commune", StringType())])


# --- BLOC : Parser le JSON

# On transforme les données brutes en données structurées en appliquant le schéma
parsed_df = raw_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")
# F.from_json() : parse le JSON en respectant le schéma
# F.col().cast() : convertit les bytes en string (car Kafka envoie des bytes)
# .select("data.*") : sélectionne toutes les colonnes du DataFrame
# explose le struct, au lieu d'une colonne data qui contient tout, on obtient une colonne par champ

 
# --- BLOC : enrichissement du DataFrame avec le watermark et les transformations

# Watermark : permet de gérer les données en retard
# Ignore les événements arrivant avec plus de 10 min de retard (libère la mémoire)

enriched_df = parsed_df.withWatermark("duedate", "10 minutes") \
    .withColumn("occupation_rate", F.round((F.col("numdocksavailable") / F.col("capacity"))*100,1)) \
    .withColumn("is_empty", F.col("numbikesavailable") == 0) \
    .withColumn("is_full", F.col("numdocksavailable") == 0)



# --- BLOC : L'agrégation fenêtrée

# window(): crée une fenêtre glissante de 5 minutes avec un pas de 2 minutes
# windowed_df : agrège les données par fenêtre et par arrondissement


windowed_df = enriched_df.groupBy(F.window("duedate", "5 minutes", "2 minutes"), F.col("nom_arrondissement_communes")) \
    .agg(F.mean("numbikesavailable").alias("avg_bikes_available"),
        F.mean("occupation_rate").alias("avg_occupation_rate"),
        F.sum(F.col("is_empty").cast("int")).alias("empty_stations"),
        F.sum(F.col("is_full").cast("int")).alias("full_stations"))


# --- BLOC : le writeStream

# query : gère le flux de données en continu
# .format("console") : affiche les données dans la console
# .outputMode("update") : met à jour les résultats dans la console
# .option("truncate", False) : affiche les données sans tronquer
# .start() : démarre le flux
# .awaitTermination() : garde le script en vie tant que le flux tourne

query = windowed_df.writeStream.format("console").outputMode("update").option("truncate", False).start()
query.awaitTermination()

