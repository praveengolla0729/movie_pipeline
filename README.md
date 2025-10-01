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



