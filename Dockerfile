FROM apache/airflow:2.8.4-python3.11

USER root
RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

# Install Python deps as 'airflow' user with official constraints
ARG AIRFLOW_VERSION=2.8.4
ARG PYTHON_VERSION=3.11
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
