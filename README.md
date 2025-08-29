## Real-Time Pipeline using Airflow, Redpanda, Pinot, Superset within Docker

Built a data warehouse, created schemas and tables for a star schema, used Airflow dags to upload them to Apache Pinot which were then visualised within Apache Superset. 

**Star Schema:**
<img width="1236" height="1036" alt="image" src="https://github.com/user-attachments/assets/47a6492d-519d-48f4-943a-69f4397701fa" />

<br><br>


Transaction facts uses RedPanda broker for realtime data streaming to Pinot allowing real time updates within Superset, which can be set at 10s refresh providing real-time visualisations.

