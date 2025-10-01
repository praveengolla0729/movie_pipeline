Title: Presentable repo cleanup + CI and contributor docs

Description:
- Added a `.gitignore` and a small `scripts/clean_repo.py` to help remove generated artifacts for presentation.
- Added CONTRIBUTING.md and LICENSE (MIT).
- Added a GitHub Actions workflow to run unit tests on push and PRs.
- Tidied README and added a short quickstart and architecture diagram.

What I tested:
- Ran `python .\scripts\clean_repo.py --remove-minio-analysis` to remove caches and logs.
- Ran unit tests locally: `python -m pytest -q` (analysis unit tests included).

Checklist for reviewers:
- [ ] Run tests locally
- [ ] Verify README quickstart
- [ ] Confirm CI runs on this branch

Notes:
- This PR removes many generated log/cache files from the repository history in a local commit. No application logic changed.
