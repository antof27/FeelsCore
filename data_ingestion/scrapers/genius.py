import lyricsgenius
import os
import json
import re
from genius_credentials import *
import time

genius = lyricsgenius.Genius(access_token)

def json_reader(path):
    
    json_files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]
    #open the json files and put the content in a list
    songs = []
    

    for file in json_files:
        with open(path + file) as json_file:
            id = file.split('_')[1]
            id = id.split('.')[0]
            
            data = json.load(json_file)
            song = data['Artists_songs']
            song = song.split('[')[0]
            song = song.lower()
            #song = song.replace(" ", "")

            if re.search(r'\b\d{4}\b', song):
                song = song[:-6]
            print(song)
            #keep the id and the song name
            song = [id, song]
            songs.append(song)
            
            
            #print(data)
    return songs
    
def json_create(string, id):
    data = {
        "Lyrics" : string,
    }

    #create the folder if not exists
    if not os.path.exists('json_lyrics'):
        os.makedirs('json_lyrics')

    filename = 'json_lyrics/' + 'lyrics_' + str(id) + '.json'
    #check if file exists
    if os.path.isfile(filename):
        return
    else:
        with open(filename, 'w') as outfile:
            json.dump(data, outfile)


def retrieve_lyrics(item):
    artist = item[1].split('-')[0]
    song = item[1].split('-')[1]
    

    artist_ = genius.search_artist(artist, max_songs=0, sort="title")
    if artist_ is None:
        print("Artist not found: " + artist)
        return
    print("Artist:" + artist)
    print("Artist: " + artist_.name)

    song_ = genius.search_song(song, artist)
    
    if song_ is None:
        return
    #print(song_.title + "----" + song)
    lyrics = song_.lyrics

    
    if lyrics is None:
        return
    
    json_create(lyrics, item[0])

k = 0
while True:

    listaa = json_reader('json_files/')
    listaa.sort(key=lambda x: x[0])
    
    for item in listaa:
        retrieve_lyrics(item)
    k = k + 1
    print("Iteration: " + str(k))
    print("Sleeping for 60 minutes...")
    time.sleep(3600)



