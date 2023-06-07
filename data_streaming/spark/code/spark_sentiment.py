from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType, StructType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, KMeansModel
import spacy
import json
from textblob import TextBlob

# Load Spacy model
nlp = spacy.load('en_core_web_sm')

def get_spark_session():
    spark_conf = SparkConf() \
        .set('spark.streaming.stopGracefullyOnShutdown', 'true') \
        .set('spark.streaming.kafka.consumer.cache.enabled', 'false') \
        .set('spark.streaming.backpressure.enabled', 'true') \
        .set('spark.streaming.kafka.maxRatePerPartition', '100') \
        .set('spark.streaming.kafka.consumer.poll.ms', '512') \
        .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1') \
        .set('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint')

    spark_session = SparkSession.builder \
        .appName('sentimentDetection') \
        .config(conf=spark_conf) \
        .getOrCreate()
    
    return spark_session

spark = get_spark_session()


def get_polarity(item):
    try:
        parsed_data = json.loads(item)
        text = parsed_data.get('Lyrics')
        if text:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            parsed_data['polarity'] = polarity
            return polarity
    except Exception as e:
        return None

def get_subjectivity(item):
    try:
        parsed_data = json.loads(item)
        text = parsed_data.get('Lyrics')
        if text:
            blob = TextBlob(text)
            subjectivity = blob.sentiment.subjectivity
            parsed_data['subjectivity'] = subjectivity
            return subjectivity
    except Exception as e:
        return None

# Define Kafka topic and server
topic = "lyricsFlux"
kafkaServer = "kafkaserver:9092"

# Read messages from Kafka
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafkaServer) \
    .option('subscribe', topic) \
    .option('startingOffsets', 'latest') \
    .load()

# Apply UDFs to the DataFrame
get_polarity_udf = udf(get_polarity, FloatType())
get_subjectivity_udf = udf(get_subjectivity, FloatType())

df = df.selectExpr("CAST(value AS STRING) AS message") \
    .withColumn("polarity", get_polarity_udf("message")) \
    .withColumn("subjectivity", get_subjectivity_udf("message"))

#Assemble the feature into a single vector of columns
assembler = VectorAssembler(inputCols=["polarity", "subjectivity"], outputCol="features")
df = assembler.transform(df)

# Train a k-means model
kmeans = KMeans().setK(4).setSeed(1)
model = kmeans.fit(df)

# Make predictions
predictions = model.transform(df)

# select all columns except features column
predictions = predictions.select([column for column in predictions.columns if column != 'features'])


# Define the output sink to display the results
query = predictions \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to terminate
query.awaitTermination()


