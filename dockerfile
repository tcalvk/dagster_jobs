FROM python:3.10-slim

# Dagster needs a home directory
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Workdir is the repo root inside the container
WORKDIR /opt/dagster/app

# Create the Dagster home directory
RUN mkdir -p $DAGSTER_HOME

# Install system deps that are often handy
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
  && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire repo into the image
COPY . .

# Make sure Python can import your Dagster package under dagster-project/src
ENV PYTHONPATH=/opt/dagster/app/dagster-project/src

# Expose Dagster webserver port
EXPOSE 3000