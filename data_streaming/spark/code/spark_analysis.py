from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType

def get_spark_session():
    spark = SparkSession.builder \
        .appName('lyricsReceiver') \
        .getOrCreate()

    return spark

spark = get_spark_session()
topic = "lyricsFlux"
kafkaServer = "kafkaserver:9092"

# Define the schema for the JSON data
schema = StructType() \
    .add("Genre", StringType()) \
    .add("Country", StringType()) \
    .add("Artists_songs", StringType()) \
    .add("Lyrics", StringType())
    #.add("Timestamp", StringType()) \
    

# Read messages from Kafka
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafkaServer) \
    .option('subscribe', topic) \
    .option('startingOffsets', 'latest') \
    .load()

# Convert value column from Kafka messages to string
df = df.withColumn('value', df['value'].cast('string'))

# Parse the JSON structure
df = df.selectExpr(
    "CAST(value AS STRING) as json",
    "timestamp"
)

df = df.select(from_json(df.json, schema).alias("data")).select("data.*")

# Print DataFrame schema
print("DataFrame Schema:")
df.printSchema()

# Write the transformed data to the console
query = df \
    .writeStream \
    .format('console') \
    .start()

print("Building DataFrame...")

query.awaitTermination()
