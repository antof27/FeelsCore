from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Set up the Kafka consumer
try:
    consumer = KafkaConsumer(
        'musicFlux',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='console-consumer-25673'
    )
    for message in consumer:
        processed_message = message.value.decode('utf-8').upper()
        print(processed_message)
except NoBrokersAvailable as e:
    print(f"Failed to connect to Kafka broker: {e}")
    # Add any other error handling logic here

'''
to add in the docker compose (not working)

  kafkaconsumer_script:
    build: 
      context: ./data_streaming/consumer_script
      dockerfile: Dockerfile
    networks:
      - tap
    volumes:
      - ./data_streaming/consumer_script:/app
    depends_on:
        kafkaConsumer:
            condition: service_completed_successfully
            
    command: python3 /app/consumer-script.py

'''