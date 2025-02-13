FROM python:3.9-slim

# Install required system packages and dependencies for ML libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/ml/processing

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download NLTK data during image build
RUN python -c "import nltk; nltk.download('punkt')"

# Create necessary directories
RUN mkdir -p /opt/ml/processing/input/data \
    /opt/ml/processing/input/code \
    /opt/ml/processing/output

# Copy processing script into the container
COPY processing_script.py /opt/ml/processing/input/code/

# Set Python path
ENV PYTHONPATH=/opt/ml/processing/input/code

# Set entrypoint
ENTRYPOINT ["python3"]