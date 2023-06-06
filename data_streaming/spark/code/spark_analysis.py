from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import sparknlp
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.pretrained import PretrainedPipeline 
from pyspark.ml import Pipeline
from sparknlp.annotator import SentenceDetector, Tokenizer, WordEmbeddingsModel, ClassifierDLApproach
from sparknlp.annotator import UniversalSentenceEncoder, ClassifierDLModel


spark = SparkSession.builder \
    .appName("Sparknlp_app") \
    .master("local[*]") \
    .config("spark.driver.memory","6G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "1000M") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.3") \
    .getOrCreate()

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

# Perform emotion analysis on the "Lyrics" column
document_assembler = DocumentAssembler() \
    .setInputCol("Lyrics") \
    .setOutputCol("document")

use = UniversalSentenceEncoder.pretrained('tfhub_use', lang="en") \
    .setInputCols(["document"])\
    .setOutputCol("sentence_embeddings")

classifier = ClassifierDLModel.pretrained('classifierdl_use_emotion', 'en') \
    .setInputCols(["sentence_embeddings"]) \
    .setOutputCol("sentiment")

nlpPipeline = Pipeline(stages=[document_assembler, use, classifier])


result = nlpPipeline.fit(parsedDf).transform(parsedDf)

result.select("sentiment.result").show(truncate=False)