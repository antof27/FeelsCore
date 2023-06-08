import os 

#go to the directory /data_ingestion/logstash and run the command: docker build . -t tap:logstash

os.system("docker build ./data_ingestion/logstash -t tap:logstash")
os.system("docker build ./data_streaming/kafka -t tap:kafka")
os.system("docker build ./data_streaming/spark -t tap:spark")
os.system("docker build ./data_visualization/kibana -t tap:kibana")
os.system("docker-compose up")
