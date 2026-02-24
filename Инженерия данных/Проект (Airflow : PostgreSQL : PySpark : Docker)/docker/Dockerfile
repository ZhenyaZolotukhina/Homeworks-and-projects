FROM apache/airflow:2.9.3-python3.10

USER root

RUN apt-get update && apt-get install -y openjdk-17-jre-headless && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pyarrow \
    pandas \
    sqlalchemy \
    psycopg2-binary
