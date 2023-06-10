from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, lit
from pyspark.sql.types import StringType, FloatType, StructType, StructField, IntegerType, DateType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import spacy
from elasticsearch import Elasticsearch
import json
from textblob import TextBlob
from datetime import datetime


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

#Define UDFs
def get_polarity(lyrics):
    blob = TextBlob(lyrics)
    polarity = blob.sentiment.polarity
    return polarity

def get_subjectivity(lyrics):
    blob = TextBlob(lyrics)
    subjectivity = blob.sentiment.subjectivity
    return subjectivity
    

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

schema = StructType([\
    StructField("Country", StringType(), True),\
    StructField("Genere", StringType(), True), \
    StructField("artists_songs", StringType(), True),\
    StructField("Lyrics", StringType(), True), \
])
    
    
# Define Elasticsearch mapping
es_mapping = {
    "mappings": {
        "properties": {
            "Country": {"type": "keyword"},
            "Genere": {"type": "keyword"},
            "@timestamp": {"type": "date"},
            "artists_songs": {"type": "keyword"},
            "Lyrics": {"type": "text"},
            "polarity": {"type": "float"},
            "subjectivity": {"type": "float"},
            "prediction": {"type": "integer"}
        }
    }
}



value_df = df.select(from_json(col("value").cast("string"), schema).alias("value"))

exploded_df = value_df.selectExpr("value.Country", "value.Genere", "value.artists_songs", "value.Lyrics")


# Apply UDFs to the DataFrame
get_polarity_udf = udf(get_polarity, FloatType())
get_subjectivity_udf = udf(get_subjectivity, FloatType())

df_sentiment = exploded_df \
    .withColumn("polarity", get_polarity_udf("Lyrics")) \
    .withColumn("subjectivity", get_subjectivity_udf("Lyrics"))

# Assemble the feature into a single vector of columns
assembler = VectorAssembler(inputCols=["polarity", "subjectivity"], outputCol="features")
df_sentiment = assembler.transform(df_sentiment)

# Create an empty DataFrame to store appended messages
appended_df = spark.createDataFrame([], df_sentiment.schema)

# Message counter
message_counter = 0
total_processed = 0


# Define the Elasticsearch index and mapping
elastic_host = "http://elasticsearch:9200"
elastic_index = "artists_songs"
es = Elasticsearch(hosts=elastic_host)

'''
response = es.indices.create(
    index=elastic_index,
    body=es_mapping,
    ignore=400 # ignore 400 already exists code
)

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])
'''

sem = False
kmeans = KMeans().setK(3).setSeed(42)
model = None

def process_batch(batch_df, batch_id):
    global appended_df
    global message_counter
    global threshold
    global sem
    global model 
    # Append the batch DataFrame to the existing DataFrame
    if sem == False:
        appended_df = appended_df.union(batch_df)

    # Increment message counter
    message_counter += batch_df.count()
    # Define the threshold of messages after which the model is trained, higher value will give better results but will take longer
    threshold = 30

    # Check if the DataFrame size is more than 5 messages
    if message_counter > threshold:
        # Train a K-means model
        if sem == False:
            print("Training model...")
            model = kmeans.fit(appended_df)
            sem = True
            

        else:
            # Make predictions
            predictions = model.transform(batch_df)

            # Select all columns except features column
            predictions = predictions.select([column for column in predictions.columns if column != 'features'])


            predictions = predictions \
                .withColumn("prediction", predictions["prediction"].cast(IntegerType())) \
                .withColumn("@timestamp", lit(datetime.now().isoformat())) \

            
            predictions.show()
           
                
            # Write the predictions to Elasticsearch
            predictions.write.format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.resource", elastic_index) \
                .option("es.mapping.id", "artists_songs") \
                .mode("append") \
                .save()
        
                
            print("Messages written to Elasticsearch: ", total_processed)
            



# Define the output sink to process the DataFrame in batches
query = df_sentiment.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Wait for the query to terminate
query.awaitTermination()