import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import os
import time
import socket

TCP_IP = 'logstash'
TCP_PORT = 5002
RETRY_DELAY = 10

#function to create the json file
def json_create(string, id):
    data = {
        "Genere" : string[0],
        "Country" : string[1],
        "artists_songs" : string[2],
        #"Timestamp" : timestamp
    }

    json_object = json.dumps(data, indent=4)
    
    #_____________UNCOMMENT THE FOLLOWING LINES TO WRITE THE JSON TO A FILE________________
    # Writing to sample.json
    # with open(f"./jsons/sample{id}.json", "w") as outfile:
    #     outfile.write(json_object)

    
    #------------UNCOMMENT THE FOLLOWING LINES TO SEND THE JSON TO LOGSTASH----------------
    # connected = False
    # while not connected:
    #     try:
    #         #json must be sent to the network
    #         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #         sock.connect((TCP_IP, TCP_PORT))
    #         sock.sendall(json.dumps(data).encode('utf-8'))
    #         sock.close()
    #         connected = True
    #     except ConnectionRefusedError:
    #         print(f"Connection to {TCP_IP}:{TCP_PORT} refused. Retrying in {RETRY_DELAY} seconds...")
    #         time.sleep(RETRY_DELAY) 





l_id = []
l_token = []
l_temp = []
genre = ""
country = ""
artists_songs = ""
local_token = []
c = 20
<<<<<<< Updated upstream
#the scraper will run forever
while True:
=======
#-----------------UNCOMMENT THE FOLLOWING LINE TO RUN THE SCRIPT-----------------
#runScript()

def runScript():
    while True:

        if c > 1:
            c = c -1
            time.sleep(1)
        else:
            
            time.sleep(60)
>>>>>>> Stashed changes

        
<<<<<<< Updated upstream
        time.sleep(60)

    url = "https://coreradio.online/page/" + str(c)
    try:
        response = requests.get(url)
    #if the response isn't correctly done, retry after 40 seconds
    except:
        time.sleep(40)
        response = requests.get(url)


    soup = BeautifulSoup(response.text, "html.parser")
    #timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content_div = soup.find("div", {"id": "dle-content"})
=======
        url = "https://coreradio.online/page/" + str(c)
        try:
            response = requests.get(url)
        #if the response isn't correctly done, retry after 40 seconds
        except:
            time.sleep(40)
            response = requests.get(url)



        soup = BeautifulSoup(response.text, "html.parser")
        #timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        content_div = soup.find("div", {"id": "dle-content"})
>>>>>>> Stashed changes

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
<<<<<<< Updated upstream
                l_id.append(id)
            
    information = content_div.text.strip()

    lines = information.splitlines()
    lines = [line for line in lines if line.strip() != ""]
    k = 0
    #div filtering
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
=======
                if k%3 == 0:
                    artists_songs = token
>>>>>>> Stashed changes
                    
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
        
        for i, j in zip(l_token, l_id):
            
            if j in local_token:    
                continue
            else:
                
                json_create(i, j)
                
            
            if c == 1 and j not in local_token:
                local_token.append(j)
                
        
        l_id = []
        l_token = []
        
                    



