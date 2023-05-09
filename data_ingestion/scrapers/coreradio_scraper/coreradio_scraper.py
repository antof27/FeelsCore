import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import os
import time
import logging
import socket

TCP_IP = 'logstash'
TCP_PORT = 5002
RETRY_DELAY = 5 

def json_create(string, id, timestamp):
    data = {
        "Genre" : string[0],
        "Country" : string[1],
        "Artists_songs" : string[2],
        "Timestamp" : timestamp
    }
    
    connected = False
    while not connected:
        try:
            #json must be sent to the network
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((TCP_IP, TCP_PORT))
            sock.sendall(json.dumps(data).encode('utf-8'))
            sock.close()
            connected = True
        except ConnectionRefusedError:
            print(f"Connection to {TCP_IP}:{TCP_PORT} refused. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY) 


    '''
    json_string = json.dumps(data)
    return json_string

    #create the folder if not exists
    if not os.path.exists('json_files'):
        os.makedirs('json_files')

    filename = 'json_files/' + 'data_' + str(id) + '.json'
    #check if file exists
    if os.path.isfile(filename):
        return
    else:
        with open(filename, 'w') as outfile:
            json.dump(data, outfile)
    


def log_create(string, id, timestamp):
    log_data = f"{timestamp} [{id}] {string[0]} - {string[1]} - {string[2]}"

    if not os.path.exists('logs'):
        os.makedirs('logs')

    filename = 'logs/' + 'data_' + str(id) + '.log'
    # check if file exists
    if os.path.isfile(filename):
        return
    else:
        with open(filename, 'w') as outfile:
            outfile.write(log_data + '\n')
    '''


l_id = []
l_token = []
l_temp = []
genre = ""
country = ""
artists_songs = ""


c = 10
while True:

    if c > 1:
        c = c -1
    else:
        time.sleep(30)

    url = "https://coreradio.online/page/" + str(c)
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content_div = soup.find("div", {"id": "dle-content"})

    a_tags = content_div.find_all('a')

    # print the href attribute of each line
    for a in a_tags:
        if "https://coreradio.online/" in a['href'] and "https://coreradio.online/page/" not in a['href'] and a['href'][-1] != "/":
            
            
            id = a['href'].split("/")[4]
            

            id = id.split("-")[0]
            id = int(id)
            print(id)
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
                genre = token.split(":")[1]
                genre = genre[1:]
                
            elif k%3 == 1:
                country = token.split(":")[1]
                country = country[1:]
                
            elif k%3 == 2:
                artists_songs = token
                l_temp = [genre, country, artists_songs, timestamp]
                if l_temp in l_token:
                    continue
                
                l_token.append(l_temp)

            k = k+1

    for i, j in zip(l_token, l_id):
        #print(i)
        print(j)

        #log_create(i, j, timestamp)
        json_create(i, j, timestamp)
    
    l_id = []
    l_token = []

                



