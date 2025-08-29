## Real-Time Pipeline using Airflow, Redpanda, Pinot, Superset within Docker

Built a data warehouse, created schemas and tables for a star schema, used Airflow dags to upload them to Apache Pinot which were then visualised within Apache Superset. 

**Architecture**

<img width="1474" height="550" alt="image" src="https://github.com/user-attachments/assets/6e3aeec6-5a26-4484-95bb-953756babeaa" />

<br></br>
<br></br>
**Superset Real-Time Visualisations**

<img width="2494" height="1174" alt="image" src="https://github.com/user-attachments/assets/9a35f2aa-aa8c-466c-818e-7a2771b0d243" />

<br></br>
<br></br>

**Star Schema:**
<img width="1000" height="860" alt="image" src="https://github.com/user-attachments/assets/47a6492d-519d-48f4-943a-69f4397701fa" />

<br><br>

**Real-Time Application:**
Transaction facts uses RedPanda broker for realtime data streaming to Pinot allowing real time updates within Superset, which can be set at 10s refresh providing real-time visualisations.

**Batch-Processing:**
Customer, Account, Date and branch dimension data is generateed daily not in real-time
<br></br>

## Steps to take:
1) Run customer, branch, date and account dim generator dags to create dimension data
2) Run schema and table dags to push json format to Apache Pinot
3) Within Pinot, use SwaggerAPI to create tables using curl, input within the body the json table format, make sure each table is successfully created
4) Run Dimension_Batch_Ingestion DAG to upload dimension data to Pinot
5) Run transaction_facts_generator to generate transaction data which will be read by RedPanda broker, and then pushed to Pinot
6) Open Superset service, connect to Pinot database, use SQLLab to combine key data from each table for a dashboard to show key visualisations
