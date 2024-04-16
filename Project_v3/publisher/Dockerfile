# Use the official Python image as the base image
FROM python:3.10-slim  

# Set the working directory in the container
WORKDIR /app
# WORKDIR /app/logs

# Copy the Python script into the container
COPY ./publisher/publisher.py .
COPY ./publisher/config.json .
# COPY ./publisher/ .

# Install any dependencies required by the publisher script
RUN pip install paho-mqtt psutil 


# Command to run the publisher script
CMD ["python", "publisher.py"]
