<<<<<<< HEAD
# Use the official Python image as the base image
FROM python:3.10-slim  

# Set the working directory in the container
WORKDIR /app

# Copy the Python script into the container
COPY ./publisher/publisher.py .
COPY ./publisher/config.py .

# Install any dependencies required by the publisher script
RUN pip install paho-mqtt psutil


# Command to run the publisher script
CMD ["python", "publisher.py"]
=======
# Use the official Python image as the base image
FROM python:3.10-slim  

# Set the working directory in the container
WORKDIR /app

# Copy the Python script into the container
COPY ./publisher/publisher.py .
COPY ./publisher/config.py .

# Install any dependencies required by the publisher script
RUN pip install paho-mqtt psutil


# Command to run the publisher script
CMD ["python", "publisher.py"]
>>>>>>> 30f65f0b13604b300aea5de2e93116e9a56e06cb
