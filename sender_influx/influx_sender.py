import paho.mqtt.client as mqtt
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import timezone
import datetime
import config

# MQTT broker details
# broker_address =   # Change this to your broker's IP address or hostname
# port = 1883  # Default MQTT port
# topic = "#"  # Topic to subscribe to

# InfluxDB details
# influx_url = "http://10.10.10.81:8086"
# influx_token = "j8LQQiHdjFiV4EVbeN6U8EOE25iBV6wKXYL_txAV-WAeTQEogb18lCS2fDx2kNH-4CQwTeEZA46xTkRzKylj4w=="
# influx_org = "seaker"
# influx_bucket = "data"

# Updating timestamp for each data


# Define callback functions

def on_connect(client, userdata, flags, rc,self):
    print("Connected with result code "+str(rc))
    # Subscribe to the topic when connected
    client.subscribe(config.topic)

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

    line_protocol = f"sensor_data,topic={msg.topic} temperature={data['temperature']},humidity={data['humidity']},memory_usage_MiB={data['memory_usage_MiB']} {utc_timestamp}"
    print(line_protocol)
    with InfluxDBClient(url=config.influx_url, token=config.influx_token, org=config.influx_org) as influx_client:
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        # point = Point("sensor_data").tag("topic", msg.topic).field("temperature", data["temperature"]).field("humidity", data["humidity"]).field("memory_usage_MiB", data["memory_usage_MiB"])
        write_api.write(bucket=config.influx_bucket, record=line_protocol,write_precision='s')
        # How precison works in influxdb

# Create MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Assign callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT broker
client.connect(config.broker_address, config.port)

# Start the loop to continuously listen for messages
client.loop_forever()