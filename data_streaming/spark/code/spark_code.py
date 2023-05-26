from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from kafka import KafkaProducer
from pyspark.sql.functions import udf
import lyricsgenius
import random as rd
import json
import re


#-------------------------- GENIUS CREDENTIALS --------------------------#
client_id = "bYn9RiVeg0XDsQxen_OhrpCqRIT3WFpUOP_cfxl41NPuybOtDzhzFDmasjiLHvgd"
client_secret = "_QzFD_y8v4ComT7DyDzEV7abtB7qTO2_k3GH3xk4G9cSb6ZpGVss5OfpM3rMwCxfteMOWj4lupVuG8b9jloTMQ"
access_token = "5eEqYSpR_WlDSv1johECV8bhePcZ-WxWxGacMWxXMzYgFhHNQWBxuDct1Fan9bYa"

#-------------------------- GENIUS SCRIPTS --------------------------#
genius = lyricsgenius.Genius(access_token, timeout=15, sleep_time=0.2, retries=5, remove_section_headers=True, skip_non_songs=True)
retry_list = []

def clean_lyrics(lyrics):
    # Remove tags
    lyrics = re.sub(r'\[.*?\]', '', lyrics)
    # Remove non-lyric text in parentheses or brackets
    lyrics = re.sub(r'\((.*?)\)|\[(.*?)\]', '', lyrics)
    # Remove any remaining parentheses or brackets
    lyrics = re.sub(r'[()\[\]]', '', lyrics)
    # Remove blank lines and leading/trailing whitespace
    lyrics = '\n'.join([line.strip() for line in lyrics.split('\n') if line.strip()])
    
    lyrics_start = lyrics.find("Lyrics")

    # If "Lyrics" is found, remove the starting artist and song title
    if lyrics_start != -1:
        lyrics = lyrics[lyrics_start + 7:]

    return lyrics



def json_create(item, string):
    parsed_data = json.loads(item)

    # Append the lyrics to the JSON object if it is not empty
    if string is not None:
        parsed_data['lyrics'] = string
    else:
        parsed_data['lyrics'] = None
        
        return

    # Convert the JSON object back to a string
    updated_item = json.dumps(parsed_data)

    # Send the updated item to Kafka
    try:
        producer = KafkaProducer(bootstrap_servers='kafkaserver:9092')
        producer.send('lyricsFlux', updated_item.encode('utf-8'))
        producer.flush()  # Wait for the message to be sent
        producer.close()  # Close the producer
        return True
    except Exception as e:
        print(f"Error sending item to Kafka: {str(e)}")
        return False

    #return item
        



def retrieve_lyrics(item):
    sem = True
    print("Item : ",item)
    parsed_data = json.loads(item)

    # Extract the value of "Artists_songs"
    artists_songs = parsed_data["Artists_songs"]
    try:
        artist = artists_songs.split('-')[0]

        song = artists_songs.split('-')[1]
        
        song = song.split('[')[0]
        song = song.lower()

        if re.search(r'\b\d{4}\b', song):
            song = song[:-6]
        
        
        artist_found = genius.search_artist(artist, sort="title", max_songs=3, allow_name_change=False)
        
        song_found = artist_found.song(song)
        

        print("SONG : ",song_found)
        print("ARTIST : ",artist_found)

        if song_found is None:
            print("la song è none!")
            sem = False
        else:
            lyrics = song_found.lyrics
            if lyrics is None:
                print("la lyrics è none!")
                sem = False
        
        if sem == False:
            if item not in retry_list:    
                print("ADDED")
                retry_list.append(item)
            
            return None

        lyrics = clean_lyrics(lyrics)

    except:
        return None


    return lyrics


def retry_songs(retry_list):
    for item in retry_list:
        lyrics = retrieve_lyrics(item)
        if lyrics is None:
            continue
        json_create(item, lyrics)
        retry_list.remove(item)


    



def get_lyrics(item):
    
    lyrics = retrieve_lyrics(item)
    #print("Lyrics : ",lyrics)
    json_create(item, lyrics)
    #call retry_songs function with a random probability

    if rd.random() < 0.1:
        retry_songs(retry_list)
        
        



    


    


    

#-------------------------- SPARK SCRIPTS --------------------------#

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
        .appName('musicReceiver') \
        .config(conf=spark_conf) \
        .getOrCreate()
    
    return spark_session

spark = get_spark_session()
topic = "musicFlux"
kafkaServer = "kafkaserver:9092"

# Read messages from Kafka
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafkaServer) \
    .option('subscribe', topic) \
    .option('startingOffsets', 'latest') \
    .load()


get_lyrics_udf = udf(get_lyrics)

# Extract the "Artists_songs" value and apply the get_lyrics function
df_with_lyrics = df.selectExpr('CAST(value AS STRING) as message') \
    .withColumn('Artists_songs', get_lyrics_udf('message'))

# Write the transformed data to the console
query = df_with_lyrics \
    .writeStream \
    .format('console') \
    .start()

query.awaitTermination()
