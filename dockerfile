ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER airflow

# Install only the extra packages needed for our DAGs
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create directory for data output with correct permissions
RUN mkdir -p /opt/airflow/data/raw && chmod -R 777 /opt/airflow/data

USER airflow