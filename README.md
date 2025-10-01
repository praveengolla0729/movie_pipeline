\# Movie Data Pipeline



This is an Apache Airflow ETL pipeline that:

\- Extracts movie data from an API

\- Uploads JSON to MinIO

\- Loads data into Postgres

\- Validates the results



\## Setup

1\. Start services:

&nbsp;  ```bash

&nbsp;  docker compose up -d



