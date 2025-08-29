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
<img width="900" height="700" alt="image" src="https://github.com/user-attachments/assets/47a6492d-519d-48f4-943a-69f4397701fa" />

<br><br>


Transaction facts uses RedPanda broker for realtime data streaming to Pinot allowing real time updates within Superset, which can be set at 10s refresh providing real-time visualisations.

