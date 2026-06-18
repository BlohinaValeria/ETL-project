# Временно упрощённый Dockerfile
FROM apache/airflow:2.8.2-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    openpyxl==3.1.2 \
    sqlalchemy==1.4.49
