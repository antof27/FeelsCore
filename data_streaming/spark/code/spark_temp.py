from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import spacy
from textblob import TextBlob

# Load Spacy model
nlp = spacy.load('en_core_web_sm')

# Create a Spark session
spark = SparkSession.builder \
    .appName("Sparknlp_app") \
    .master("local[*]") \
    .config("spark.driver.memory", "16G") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "1000M") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.3") \
    .getOrCreate()

#create a dataframe with some differents instances of text
df = spark.createDataFrame([
    ("I had a really horrible day. It was the worst day ever! But every now and then I have a really good day that makes me happy.", ),
    ("I had a really good day. It was the worst day ever! But every now and then I have a really good day that makes me happy.", ), 
    ("I had a really beaufitul day. It was the best day ever! But every now and then I have a really good day that makes me happy.", ),
    ("I had a really bad day. But every now and then I have a really good day that makes me happy.", ),
], ["Lyrics"])
    
    



# Define UDFs to calculate polarity and subjectivity
@udf(returnType=FloatType())
def get_polarity(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

@udf(returnType=FloatType())
def get_subjectivity(text):
    blob = TextBlob(text)
    return blob.sentiment.subjectivity

# Append polarity and subjectivity columns to the DataFrame
df = df.withColumn("polarity", get_polarity(df["Lyrics"].cast(StringType())))
df = df.withColumn("subjectivity", get_subjectivity(df["Lyrics"].cast(StringType())))

# Assemble the features into a single vector column
assembler = VectorAssembler(inputCols=["polarity", "subjectivity"], outputCol="features")
featureDf = assembler.transform(df)

# Apply K-means clustering
kmeans = KMeans(k=3, seed=42)
model = kmeans.fit(featureDf)

# Make predictions
predictions = model.transform(featureDf)

# Select only the necessary columns and rename "prediction" column to "group"
resultDf = predictions.select("Lyrics", "polarity", "subjectivity", "prediction") \
                      .withColumnRenamed("prediction", "group")

# Show the final result
resultDf.show()

# Stop the Spark session
spark.stop()