Local end-to-end testing without Airflow
-------------------------------------

You can run the pipeline end-to-end against the bundled Docker Compose (Postgres + MinIO) and use the convenience script to execute tasks without Airflow.

1. Start services:

```powershell
docker compose up -d postgres minio
```

2. Wait for services to be healthy, then run the local runner (uses the same task functions as the DAG):

```powershell
# in repo root
python .\scripts\run_local_pipeline.py
```

This will extract from the sample API, upload to MinIO, load into Postgres, run analysis and validation. Analysis output is written to MinIO and to `minio_data/<bucket>/analysis/latest.json` for easy inspection.
