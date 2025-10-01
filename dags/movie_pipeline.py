from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import psycopg2
import json
import os

# ================
# CONFIGURATIONS
# ================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
MINIO_BUCKET = "movie-bucket"

POSTGRES_CONN = {
    "host": "postgres",
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432
}

# ================
# TASK FUNCTIONS
# ================

# 1. Extract movies from API
def extract_from_api():
    url = "https://api.sampleapis.com/movies/action"
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise Exception(f"API failed with status {response.status_code}")
    
    try:
        data = response.json()
    except Exception:
        raise Exception("Failed to parse API response as JSON")

    # Save to local file
    with open("/tmp/movies.json", "w") as f:
        json.dump(data, f)
    return "Movies extracted successfully"

# 2. Upload file to MinIO with versioned name
def upload_to_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # Check bucket exists (or create it)
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except:
        s3.create_bucket(Bucket=MINIO_BUCKET)

    # Versioned file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"movies_{timestamp}.json"

    s3.upload_file("/tmp/movies.json", MINIO_BUCKET, key)
    return f"Uploaded {key} to MinIO"

# 3. Load data into Postgres
def load_to_postgres():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id SERIAL PRIMARY KEY,
            title TEXT,
            year INT,
            genres TEXT
        )
    """)

    with open("/tmp/movies.json", "r") as f:
        movies = json.load(f)

    for movie in movies:
        cur.execute(
            "INSERT INTO movies (title, year, genres) VALUES (%s, %s, %s)",
            (movie.get("title"), movie.get("year"), str(movie.get("genres")))
        )

    conn.commit()
    cur.close()
    conn.close()
    return "Data loaded into Postgres"

# 4. Data validation (extra step)
def validate_data():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    # Check if data exists
    cur.execute("SELECT COUNT(*) FROM movies;")
    count = cur.fetchone()[0]

    if count == 0:
        raise ValueError("❌ No data found in movies table!")

    # Check for null titles
    cur.execute("SELECT COUNT(*) FROM movies WHERE title IS NULL;")
    null_count = cur.fetchone()[0]

    if null_count > 0:
        raise ValueError(f"❌ Found {null_count} movies with null titles")

    conn.close()
    return f"✅ Validation passed. Row count = {count}"

# ================
# DAG DEFINITION
# ================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="movie_data_pipeline",
    default_args=default_args,
    description="ETL pipeline for movies",
    schedule_interval="@daily",  # <--- Daily run
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="extract_from_api",
        python_callable=extract_from_api,
    )

    task2 = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )

    task3 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    task4 = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    # Set task dependencies
    task1 >> task2 >> task3 >> task4
