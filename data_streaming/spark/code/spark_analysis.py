
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob

# Load Spacy model
nlp = spacy.load('en_core_web_sm')

text_blob = SpacyTextBlob(nlp)


# Create a Spark session
spark = SparkSession.builder \
    .appName("Sparknlp_app") \
    .master("local[*]") \
    .config("spark.driver.memory", "16G") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "1000M") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.3") \
    .getOrCreate()


# Sample text
# Sample text
text = "I had a really horrible day. It was the worst day ever! But every now and then I have a really good day that makes me happy."

# Create a DataFrame with the sample text
df = spark.createDataFrame([(text,)], ["Lyrics"])

# Define UDFs to calculate polarity and subjectivity
get_polarity = udf(lambda text: text_blob(text).polarity, FloatType())
get_subjectivity = udf(lambda text: text_blob(text).subjectivity, FloatType())

# Append polarity and subjectivity columns to the DataFrame
df = df.withColumn("polarity", get_polarity(df["Lyrics"].cast(StringType())))
df = df.withColumn("subjectivity", get_subjectivity(df["Lyrics"].cast(StringType())))

# Show the result
df.show()

# Stop the Spark session
spark.stop()




'''
#Put over udf functions

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

# Define the schema for the Kafka message
messageSchema = StructType().add("Lyrics", StringType())

# Parse the Kafka message as JSON and select the "Lyrics" column
parsedDf = df.select(from_json(col("value").cast("string"), messageSchema).alias("parsed_value")) \
             .select("parsed_value.Lyrics")
'''