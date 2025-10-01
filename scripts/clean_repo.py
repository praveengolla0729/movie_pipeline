"""Simple cleanup helper to remove generated artifacts for presentation.

This script will remove:
- __pycache__ directories
- .pyc files
- logs/ directory
- .pytest_cache/
- optionally minio_data/movie-bucket/analysis (if --remove-minio-analysis passed)

Run from repo root: python scripts/clean_repo.py [--remove-minio-analysis]
"""
import argparse
import os
import shutil
from pathlib import Path


def remove_path(path: Path):
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
        print(f"Removed directory: {path}")
    else:
        path.unlink()
        print(f"Removed file: {path}")


def find_and_remove_pyc(repo_root: Path):
    for p in repo_root.rglob('*.pyc'):
        try:
            p.unlink()
            print(f"Removed: {p}")
        except Exception as e:
            print(f"Failed to remove {p}: {e}")


def find_and_remove_pycache(repo_root: Path):
    for d in repo_root.rglob('__pycache__'):
        try:
            shutil.rmtree(d)
            print(f"Removed __pycache__: {d}")
        except Exception as e:
            print(f"Failed to remove {d}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--remove-minio-analysis', action='store_true', help='Also remove minio_data/movie-bucket/analysis')
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    print(f"Cleaning repo at {repo_root}")

    # Remove logs
    logs_dir = repo_root / 'logs'
    remove_path(logs_dir)

    # Remove pytest cache
    remove_path(repo_root / '.pytest_cache')

    # Remove pyc files and __pycache__
    find_and_remove_pyc(repo_root)
    find_and_remove_pycache(repo_root)

    # Optionally remove minio analysis outputs
    if args.remove_minio_analysis:
        analysis_dir = repo_root / 'minio_data' / 'movie-bucket' / 'analysis'
        remove_path(analysis_dir)

    print('Cleanup complete.')


if __name__ == '__main__':
    main()
