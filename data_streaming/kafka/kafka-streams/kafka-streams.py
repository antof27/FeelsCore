from kafka import KafkaConsumer, KafkaProducer
from kafka import KafkaStreams
from kafka import TopicPartition
from kafka import StreamBuilder, StringSerde, AutoOffsetReset
import genius

# Create the StreamsBuilder
builder = StreamBuilder.default_builder()

# Set up the input topic and output topic
input_topic = "musicFlux"
output_topic = "lyricsFlux"

# Create a source stream from the input topic
source_stream = builder.stream(input_topic, value_serde=StringSerde())

# Apply the capitalization transformation
capitalized_stream = source_stream.map_values(genius.get_lyrics)

# Write the capitalized data to the output topic
capitalized_stream.to(output_topic, value_serde=StringSerde())

# Build the Kafka Streams application
streams = builder.build()

# Start the Kafka Streams application
streams.start()

# Create a Kafka producer
# producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Create a Kafka consumer
# consumer = KafkaConsumer(bootstrap_servers="localhost:9092", auto_offset_reset=AutoOffsetReset.EARLIEST)
# consumer.subscribe([output_topic])
# Stop the Kafka Streams application
streams.close()