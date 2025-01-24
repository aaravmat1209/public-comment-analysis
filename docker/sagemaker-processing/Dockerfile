# Use Python 3.9 slim as base
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install additional requirements
COPY requirements.txt /opt/ml/requirements.txt
RUN pip install -r /opt/ml/requirements.txt

# Set up working directory
WORKDIR /opt/ml/processing

# Set Python path
ENV PYTHONPATH=/opt/ml/processing