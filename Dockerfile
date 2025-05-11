# Use Apache Airflow base image
FROM apache/airflow:2.7.3-python3.9

# Install OS-level dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    python3-dev \
    libffi-dev \
    libssl-dev \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch to airflow user BEFORE installing Python packages
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

