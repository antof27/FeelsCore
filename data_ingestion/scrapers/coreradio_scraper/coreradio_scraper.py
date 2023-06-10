import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import os
import time
import socket

TCP_IP = 'logstash'
TCP_PORT = 5002
RETRY_DELAY = 60

def json_create(string, id):
    data = {
        "Genere" : string[0],
        "Country" : string[1],
        "artists_songs" : string[2],
        #"Timestamp" : timestamp
    }
    
    connected = False
    while not connected:
        try:
            # Create a TCP/IP socket and connect to Logstash
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((TCP_IP, TCP_PORT))
            sock.sendall(json.dumps(data).encode('utf-8'))
            sock.close()
            connected = True
        except ConnectionRefusedError:
            print(f"Connection to {TCP_IP}:{TCP_PORT} refused. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY) 





l_id = []
l_token = []
l_temp = []
genre = ""
country = ""
artists_songs = ""
local_token = []
c = 20
while True:

    if c > 1:
        c = c -1
        time.sleep(4)
    else:
        
        time.sleep(3600)

    
    url = "https://coreradio.online/page/" + str(c)
    try:
        response = requests.get(url)
    #if the response isn't correctly done, retry after 60 seconds
    except:
        time.sleep(60)
        response = requests.get(url)



    soup = BeautifulSoup(response.text, "html.parser")
    #timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content_div = soup.find("div", {"id": "dle-content"})

    a_tags = content_div.find_all('a')

    # print the href attribute of each line
    for a in a_tags:
        if "https://coreradio.online/" in a['href'] and "https://coreradio.online/page/" not in a['href'] and a['href'][-1] != "/":
            
            
            id = a['href'].split("/")[4]
            

            id = id.split("-")[0]
            id = int(id)
            #print(id)
            if id is None or id in l_id:
                continue
            else:
                l_id.append(id)
            
    information = content_div.text.strip()

    lines = information.splitlines()
    lines = [line for line in lines if line.strip() != ""]
    k = 0
    for token in lines:

        if token == "more" or token == "MAIN" or token == '«' or token == '»' or token == "Load more" or "Quality:" in token or len(token) <2:
            continue
        else:
            if k%3 == 0:
                artists_songs = token
                
            elif k%3 == 1:
                #before making the split ensure there is a ":" in the string
                try:
                    genre = token.split(":")[1]
                    
                except:
                    continue
                
            elif k%3 == 2:
                try: 
                    country = token.split(":")[1]
                except:
                    continue
                
                l_temp = [genre, country, artists_songs]
                if l_temp in l_token:
                    continue
                
                l_token.append(l_temp)

            k = k+1
    # for every token in the list, check if it is already in the local_token list
    for i, j in zip(l_token, l_id):
        
        if j in local_token:    
            continue
        else:
            
            json_create(i, j)
            
        
        if c == 1 and j not in local_token:
            local_token.append(j)
            
    
    l_id = []
    l_token = []
    
                



