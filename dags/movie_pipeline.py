try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except Exception:
    # Airflow may not be available on Windows or in lightweight dev environments
    AIRFLOW_AVAILABLE = False
from datetime import datetime, timedelta
import requests
import boto3
import psycopg2
import json
import os
from dags.analysis import analyze_movies_from_list
from dags.analysis import data_quality_report, compute_genre_similarity_recommendations
from dags.alerts import check_and_alert_dq


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

    import tempfile
    tmp_dir = tempfile.gettempdir()
    tmp_path = os.path.join(tmp_dir, "movies.json")

    # Save to local file
    with open(tmp_path, "w") as f:
        json.dump(data, f)
    # expose where we wrote so local runner can find it (but keep original return)
    return json.dumps({"message": "Movies extracted successfully", "path": tmp_path})

# 2. Upload file to MinIO with versioned name
def upload_to_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # Check bucket exists (or try to create it). If MinIO is unreachable we'll fallback to local mirror.
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        try:
            s3.create_bucket(Bucket=MINIO_BUCKET)
        except Exception:
            # MinIO unreachable: continue, upload will fallback
            pass

    # Versioned file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"movies_{timestamp}.json"

    import tempfile
    tmp_path = os.path.join(tempfile.gettempdir(), "movies.json")
    if not os.path.exists(tmp_path):
        raise FileNotFoundError(f"Expected movies.json at {tmp_path}. Run extract first.")

    # Try upload; fallback to writing local mirror when MinIO not reachable
    try:
        s3.upload_file(tmp_path, MINIO_BUCKET, key)
        return f"Uploaded {key} to MinIO"
    except Exception:
        local_dir = os.path.join("minio_data", MINIO_BUCKET)
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, key)
        with open(tmp_path, "rb") as src, open(dest, "wb") as dst:
            dst.write(src.read())
        return f"MinIO unreachable, wrote local mirror {dest}"

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

    import tempfile
    tmp_path = os.path.join(tempfile.gettempdir(), "movies.json")
    with open(tmp_path, "r", encoding="utf-8") as f:
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


# 5. Analysis task
def analyze_movies():
    """Read movies from Postgres, compute analysis, save to MinIO and insert into analytics table."""
    # Read from Postgres
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    cur.execute("SELECT title, year, genres FROM movies;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    movies = []
    for title, year, genres in rows:
        movies.append({"title": title, "year": year, "genres": genres})

    analysis = analyze_movies_from_list(movies)

    # Persist to MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Try to upload to MinIO; if unreachable write local mirror files so developer can inspect outputs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_key = f"analysis/analysis_{timestamp}.json"
    latest_key = "analysis/latest.json"
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        s3.put_object(Bucket=MINIO_BUCKET, Key=json_key, Body=json.dumps(analysis).encode("utf-8"))
        s3.put_object(Bucket=MINIO_BUCKET, Key=latest_key, Body=json.dumps(analysis).encode("utf-8"))
    except Exception:
        try:
            local_dir = os.path.join("minio_data", MINIO_BUCKET, "analysis")
            os.makedirs(local_dir, exist_ok=True)
            with open(os.path.join(local_dir, f"analysis_{timestamp}.json"), "w", encoding="utf-8") as f:
                json.dump(analysis, f)
            with open(os.path.join(local_dir, "latest.json"), "w", encoding="utf-8") as f:
                json.dump(analysis, f)
        except Exception:
            pass

    # Insert summary into Postgres analytics table
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics (
            id SERIAL PRIMARY KEY,
            name TEXT,
            payload JSONB,
            generated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
        )
    """)
    cur.execute(
        "INSERT INTO analytics (name, payload) VALUES (%s, %s)",
        ("movie_summary", json.dumps(analysis)),
    )
    conn.commit()
    cur.close()
    conn.close()

    return {"json_key": json_key}


def run_data_quality_report():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    cur.execute("SELECT title, year, genres FROM movies;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    movies = [{"title": t, "year": y, "genres": g} for t, y, g in rows]
    report = data_quality_report(movies)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    key = f"analysis/data_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=json.dumps(report).encode("utf-8"))
    except Exception:
        try:
            local_dir = os.path.join("minio_data", MINIO_BUCKET, "analysis")
            os.makedirs(local_dir, exist_ok=True)
            with open(os.path.join(local_dir, "data_quality_latest.json"), "w", encoding="utf-8") as f:
                json.dump(report, f)
        except Exception:
            pass

    # Check thresholds and optionally alert via Slack
    slack_url = os.environ.get("SLACK_WEBHOOK")
    check_and_alert_dq(report, slack_webhook_url=slack_url, completeness_threshold=0.8, duplicate_threshold=5)

    return {"dq_key": key}


def compute_recommendations():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    cur.execute("SELECT title, year, genres FROM movies;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    movies = [{"title": t, "year": y, "genres": g} for t, y, g in rows]
    recs = compute_genre_similarity_recommendations(movies, top_n=5)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    key = f"analysis/recommendations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=json.dumps(recs).encode("utf-8"))
    except Exception:
        try:
            local_dir = os.path.join("minio_data", MINIO_BUCKET, "analysis")
            os.makedirs(local_dir, exist_ok=True)
            with open(os.path.join(local_dir, "recommendations_latest.json"), "w", encoding="utf-8") as f:
                json.dump(recs, f)
        except Exception:
            pass

    return {"recs_key": key}

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

if AIRFLOW_AVAILABLE:
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

        task5 = PythonOperator(
            task_id="analyze_movies",
            python_callable=analyze_movies,
        )

        task6 = PythonOperator(
            task_id="data_quality_report",
            python_callable=run_data_quality_report,
        )

        task7 = PythonOperator(
            task_id="compute_recommendations",
            python_callable=compute_recommendations,
        )

        task4 = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
        )

        # Set task dependencies
        task1 >> task2 >> task3 >> task5 >> task6 >> task7 >> task4
