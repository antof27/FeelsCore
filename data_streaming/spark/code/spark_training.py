from typing_extensions import StrictTypeGuard
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, CountVectorizer
from pyspark.ml.classification import RandomForestClassifier

# Initialize Spark session
spark = SparkSession.builder.appName("EmotionAnalysis").getOrCreate()

# Set the path to the volume directory containing JSON files
volume_path = "/app/lyrics"

# Define the schema for the JSON data (replace with your actual schema)
schema = StrictTypeGuard() \
    .add("Genre", StringType()) \
    .add("Country", StringType()) \
    .add("Artists_songs", StringType()) \
    .add("Lyrics", StringType())

# Read JSON files from the volume directory
df = spark.read.schema(schema).json(volume_path)

# Check the number of JSON files in the volume
num_json_files = df.count()
if num_json_files >= 100:
    # Build the dataset and train the model

    # Tokenize the "Lyrics" column
    tokenizer = Tokenizer(inputCol="Lyrics", outputCol="tokens")
    df_tokenized = tokenizer.transform(df)

    # Count vectorize the tokens
    count_vectorizer = CountVectorizer(inputCol="tokens", outputCol="features")
    df_vectorized = count_vectorizer.fit(df_tokenized).transform(df_tokenized)

    # Define the model
    classifier = RandomForestClassifier(labelCol="Emotion", featuresCol="features")

    # Create a pipeline for the transformation and model
    pipeline = Pipeline(stages=[tokenizer, count_vectorizer, classifier])

    # Train the model
    model = pipeline.fit(df_vectorized)

    # Make predictions
    df_predictions = model.transform(df_vectorized)

    # Show the resulting dataset with predictions
    df_predictions.show()
else:
    # Wait for more JSON files to be added to the volume
    print("Waiting for more JSON files to be added to the volume...")
