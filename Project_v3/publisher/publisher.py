import paho.mqtt.client as mqtt
import json
import random
import time
import psutil
import os
import logging


# Make new path(dir)
try:
    newpath = r'log_files' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    print(f"file Created...{newpath}")
except Exception as e:
    print(e)

logging.basicConfig(filename=str('log_files/publisher_logs.log'), level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s', filemode='a')


# process_name = "publisher.py"
# process_info = []

pid = os.getpid()

# Log File Settings


# Read the json file
with open('config.json','r') as f:
    config = json.load(f)

broker_address = config['publisher']['broker_address']
port = config['publisher']['port']
topic = config['publisher']['topic']

# Create MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Connect to MQTT broker
try:
    client.connect(broker_address, port)
    logging.info(f"Connected to Broker{broker_address}")
except Exception as e:
    logging.error(e)


# Create JSON payload template
payload_template = {
    "topic": topic
}

# Publish temperature and humidity values every 10 seconds
while True:
    start_time = time.time()  # Start time for the iteration

    # Generate random temperature and humidity values
    temperature = round(random.uniform(20, 30), 2)  # Random temperature between 20 and 30 Celsius
    humidity = round(random.uniform(40, 60), 2)     # Random humidity between 40% and 60%
    
    
    # Get process memory usage
    process_memory = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)  # Memory usage in MiB

    # Update JSON payload with new data
    payload_template["temperature"] = temperature
    payload_template["humidity"] = humidity
    payload_template['container_pid'] = process_memory

    # Serialize JSON data
    payload = json.dumps(payload_template)

    # Publish payload to MQTT broker
    client.publish(topic, payload)
    # logging.info("Publishing")


    # Calculate time spent and adjust sleep time to maintain 5-second interval
    elapsed_time = time.time() - start_time
    sleep_time = max(0, 5 - elapsed_time)  # Ensure non-negative sleep time
    time.sleep(sleep_time)
# logging.info("publishing")
# Disconnect from MQTT broker
client.disconnect()


