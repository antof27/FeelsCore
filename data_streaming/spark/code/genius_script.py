import lyricsgenius
import json
import re
from genius_credentials import *
genius = lyricsgenius.Genius(access_token)


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

def json_reader(json_file):
    
    with open(json_file) as f:   

        id = json_file.split('_')[1]
        id = id.split('.')[0]
        
        data = json.load(f)
        song = data['Artists_songs']
        song = song.split('[')[0]
        song = song.lower()

        if re.search(r'\b\d{4}\b', song):
            song = song[:-6]

        song = [id, song]
        
    return song


def json_create(json_file, string):
    # append the string lyrics to the json file if it is not empty
    if string is not None:
        with open(json_file, 'r') as f:
            data = json.load(f)
            data['lyrics'] = string
        '''
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=4)
        '''    
        return json_file
    else:
        # add a field in the json file, called lyrics for the song
        with open(json_file, 'r') as f:
            data = json.load(f)
            data['lyrics'] = None
        '''
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=4)
        '''
        return json_file
        



def retrieve_lyrics(item):
    artist = item[1].split('-')[0]
    song = item[1].split('-')[1]
    

    artist_ = genius.search_artist(artist, max_songs=0, sort="title")
    

    artist_low = artist_.name.lower()
    artist_low = artist_low.replace(" ", "")
    artist = artist.replace(" ", "")
    print(artist_low,".")
    print(artist, ".")
    if artist_low != artist:
        print("Artist not found: " + artist)
        return None
    
    song_ = genius.search_song(song, artist)
    
    if song_ is None:
        return None

    lyrics = song_.lyrics
    
    if lyrics is None:
        return None
    
    lyrics = clean_lyrics(lyrics)

    return lyrics



def get_lyrics(item):
    
    lyrics = retrieve_lyrics(item)
  
    song = json_reader(item)
    lyrics = retrieve_lyrics(song)
    json_create(item, lyrics)

    return item


