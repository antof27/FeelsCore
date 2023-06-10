from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from kafka import KafkaProducer
from pyspark.sql.functions import udf
import lyricsgenius
import numpy as np
import random as rd
import time
import json
import os
import re


#-------------------------- GENIUS CREDENTIALS --------------------------#
client_id = "bYn9RiVeg0XDsQxen_OhrpCqRIT3WFpUOP_cfxl41NPuybOtDzhzFDmasjiLHvgd"
client_secret = "_QzFD_y8v4ComT7DyDzEV7abtB7qTO2_k3GH3xk4G9cSb6ZpGVss5OfpM3rMwCxfteMOWj4lupVuG8b9jloTMQ"
access_token = "5eEqYSpR_WlDSv1johECV8bhePcZ-WxWxGacMWxXMzYgFhHNQWBxuDct1Fan9bYa"

#-------------------------- GENIUS SCRIPTS --------------------------#
genius = lyricsgenius.Genius(access_token, timeout=15, sleep_time=0.2, retries=5, remove_section_headers=True, skip_non_songs=True)
retry_list = []
last_retry_time = time.time()

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
    sem = False
    # Append the lyrics to the JSON object if it is not empty
    print("String arrived : ",string)
    if string is not None:
        parsed_data['Lyrics'] = string
        sem = True
    else:

        parsed_data['Lyrics'] = None
        
        

    # Convert the JSON object back to a string
    updated_item = json.dumps(parsed_data)

    # Send the updated item to Kafka
    try:
        if sem == True:

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

    artists_songs = parsed_data["artists_songs"]
    try:
        artist = artists_songs.split('-')[0]

        song = artists_songs.split('-')[1]
        
        song = song.split('[')[0]
        song = song.lower()

        if re.search(r'\b\d{4}\b', song):
            song = song[:-6]
        
        
        artist_found = genius.search_artist(artist, sort="title", max_songs=3, allow_name_change=False)
        
        song_found = artist_found.song(song)
        
        if song_found is None:
            sem = False
        else:
            lyrics = song_found.lyrics
            if lyrics is None:
                sem = False
        
        if sem == False:
            if item not in retry_list:    
                
                retry_list.append(item)
            
            return None

        lyrics = clean_lyrics(lyrics)
        if lyrics.endswith('Embed'):
            lyrics = lyrics[:-5]

    except:
        return None


    return lyrics


def retry_songs(retry_list):
    if len(retry_list) == 0:
           return

    for item in retry_list:
        lyrics = retrieve_lyrics(item)
        
        if lyrics is None:
            continue
        else:
            json_create(item, lyrics)
            retry_list.remove(item)

    len_list = len(retry_list)
    elements_to_remove = int((10*len_list)/100)
    prob = np.linspace(0, 1, elements_to_remove)
    prob = 1 - prob
    
    for i in range(elements_to_remove):
        if rd.random() < prob[i]:
    
            retry_list.remove(retry_list[i])
            len_list = len_list - 1
        else:
            continue

    
            

def get_lyrics(item):
    global last_retry_time 
    lyrics = retrieve_lyrics(item)
    if lyrics is not None:
        json_create(item, lyrics)

    current_time = time.time()
    if current_time - last_retry_time >= (60*60*24):  
        print("Retrying 24 hours later...")
        retry_songs(retry_list)
        last_retry_time = current_time

        
        

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
    .withColumn('artists_songs', get_lyrics_udf('message'))

# Write the transformed data to the console
query = df_with_lyrics \
    .writeStream \
    .format('console') \
    .start()

query.awaitTermination()
