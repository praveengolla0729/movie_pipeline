# Movie Pipeline

Small ETL + analytics pipeline for movie data.

Structure

- `dags/`: Airflow DAGs and helper modules (usable directly for local testing).
- `scripts/`: helpers for running and cleaning the repo.
- `tests/`: unit tests for analysis code.

Quick commands

Run tests:

```powershell
python -m pytest -q
```

Clean generated files:

```powershell
python .\scripts\clean_repo.py --remove-minio-analysis
```

See `RUN_LOCAL.md` for local-run instructions.

CI
--
This repository includes a GitHub Actions workflow that runs unit tests on push and pull requests. See `.github/workflows/python-tests.yaml`.

License
--
This project is available under the MIT License (see `LICENSE`).

Quickstart
--
1. Clone the repo and start local services (Docker Compose):

```powershell
docker compose up -d postgres minio
```

2. Run the lightweight local runner (no Airflow required):

```powershell
python .\scripts\run_local_pipeline.py
```

3. Inspect outputs:
- MinIO mirror (local): `minio_data/movie-bucket/analysis/latest.json`
- Local sample tests: `python -m pytest -q`

Architecture (ASCII)
--
	[extract] -> [upload to MinIO] -> [load to Postgres] -> [analysis tasks] -> [analytics stored in MinIO & Postgres]

Notes
--
- DAGs are under `dags/` — compute helpers are factored into `dags/analysis.py` so they can be run and tested without Airflow installed.
- For presentation, run `python .\scripts\clean_repo.py` to remove logs and caches.


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



