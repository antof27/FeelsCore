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

df_sentiment = df.selectExpr("CAST(value AS STRING) AS message") \
    .withColumn("polarity", get_polarity_udf("message")) \
    .withColumn("subjectivity", get_subjectivity_udf("message"))

# Assemble the feature into a single vector of columns
assembler = VectorAssembler(inputCols=["polarity", "subjectivity"], outputCol="features")
df_sentiment = assembler.transform(df_sentiment)

# Create an empty DataFrame to store appended messages
appended_df = spark.createDataFrame([], df_sentiment.schema)

# Message counter
message_counter = 0

def process_batch(batch_df, batch_id):
    global appended_df
    global message_counter

    # Append the batch DataFrame to the existing DataFrame
    appended_df = appended_df.union(batch_df)

    # Increment message counter
    message_counter += batch_df.count()

    # Check if the DataFrame size is more than 5 messages
    if message_counter >= 5:
        # Train a K-means model
        kmeans = KMeans().setK(4).setSeed(1)
        model = kmeans.fit(appended_df)

        # Make predictions
        predictions = model.transform(appended_df)

        # Select all columns except features column
        predictions = predictions.select([column for column in predictions.columns if column != 'features'])

        # Display the results
        predictions.show()

        # Clear the appended DataFrame
        appended_df = spark.createDataFrame([], df_sentiment.schema)

        # Reset the message counter
        message_counter = 0


# Define the output sink to process the DataFrame in batches
query = df_sentiment.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Wait for the query to terminate
query.awaitTermination()
