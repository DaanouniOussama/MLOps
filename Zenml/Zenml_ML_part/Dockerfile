# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Define environment variable for MLflow tracking
ENV MLFLOW_TRACKING_URI http://your-mlflow-tracking-server:5000

# Run the training script when the container launches
CMD ["python", "train.py"]
