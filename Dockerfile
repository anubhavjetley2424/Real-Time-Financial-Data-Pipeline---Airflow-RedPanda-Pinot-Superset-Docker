FROM apache/airflow:2.7.1

# Install kafka-python, pandas, and requests
RUN pip install kafka-python pandas requests