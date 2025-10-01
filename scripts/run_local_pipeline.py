"""Run the pipeline tasks locally without Airflow.

This script imports the task functions from the DAG module and runs them in order.
It is intended for local development when Docker Compose is running Postgres and MinIO.
"""
import time
import sys
import os

# Ensure repo root is on sys.path so `dags` package can be imported when running this script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dags.movie_pipeline import (
    extract_from_api,
    upload_to_minio,
    load_to_postgres,
    analyze_movies,
    validate_data,
)


def main():
    print("Running extract_from_api()...")
    extract_from_api()
    time.sleep(1)

    print("Uploading to MinIO...")
    upload_to_minio()
    time.sleep(1)

    print("Loading to Postgres...")
    load_to_postgres()
    time.sleep(1)

    print("Running analysis...")
    out = analyze_movies()
    print("Analysis saved:", out)
    time.sleep(1)

    print("Validating data...")
    print(validate_data())


if __name__ == "__main__":
    main()
