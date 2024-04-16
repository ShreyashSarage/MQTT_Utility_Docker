# Use the official Python image as the base image
FROM python:3.10-slim   

# Set the working directory in the container
WORKDIR /app

# Copy the Python script into the container
COPY ./sender_influx/influx_sender.py .
COPY ./sender_influx/config.json .

# Install any dependencies required by the subscriber script
RUN pip install paho-mqtt influxdb-client

# Command to run the subscriber script
CMD ["python", "influx_sender.py"]
