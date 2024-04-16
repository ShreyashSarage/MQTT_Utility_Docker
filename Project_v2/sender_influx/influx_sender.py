import paho.mqtt.client as mqtt
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import timezone
import datetime

# MQTT broker details
# broker_address =   # Change this to your broker's IP address or hostname
# port = 1883  # Default MQTT port
# topic = "#"  # Topic to subscribe to

# InfluxDB details
# influx_url = "http://10.10.10.81:8086"
# influx_token = "j8LQQiHdjFiV4EVbeN6U8EOE25iBV6wKXYL_txAV-WAeTQEogb18lCS2fDx2kNH-4CQwTeEZA46xTkRzKylj4w=="
# influx_org = "seaker"
# influx_bucket = "data"


# Read the json file
with open('config.json','r') as f:
    config = json.load(f)

broker_address = config['publisher']['broker_address']
port = config['publisher']['port']
topic = config['publisher']['topic']


influx_url =  config['influxdb']['influx_url']
influx_token = config['influxdb']['influx_token']
influx_org = config['influxdb']['influx_org']
influx_bucket =  config['influxdb']['influx_bucket']

# Define callback functions

def on_connect(client, userdata, flags, rc,self):
    print("Connected with result code "+str(rc))
    # Subscribe to the topic when connected
    client.subscribe(topic)

def on_message(client, userdata, msg):
    # Decode the message payload from bytes to string
    message = msg.payload.decode("utf-8")

    # dt = datetime.datetime.utcnow()

    # timestamp = int(dt.timestamp())

    dt = datetime.datetime.now(timezone.utc)
 
    utc_time = dt.replace(tzinfo=timezone.utc)
    utc_timestamp = int(utc_time.timestamp())
    # Deserialize the JSON message
    data = json.loads(message)

    # timestamp = int(msg.timestamp)

    line_protocol = f"sensor_data,topic={msg.topic} temperature={data['temperature']},humidity={data['humidity']} {utc_timestamp}"
    print(line_protocol)
    with InfluxDBClient(url=influx_url, token=influx_token, org=influx_org) as influx_client:
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        # point = Point("sensor_data").tag("topic", msg.topic).field("temperature", data["temperature"]).field("humidity", data["humidity"]).field("memory_usage_MiB", data["memory_usage_MiB"])
        write_api.write(bucket=influx_bucket, record=line_protocol,write_precision='s')
        # How precison works in influxdb

# Create MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Assign callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT broker
client.connect(broker_address, port)

# Start the loop to continuously listen for messages
client.loop_forever()