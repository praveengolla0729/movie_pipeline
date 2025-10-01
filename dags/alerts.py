import requests
from typing import Optional


def check_and_alert_dq(report: dict, slack_webhook_url: Optional[str] = None, completeness_threshold: float = 0.9, duplicate_threshold: int = 0):
    """Check data quality report against thresholds. If a threshold is violated, send Slack alert (if webhook provided) and raise Exception.

    Returns None on success. Raises Exception on failure.
    """
    failures = []
    completeness = float(report.get("completeness", 0.0))
    dup_count = int(report.get("duplicate_title_count", 0))

    if completeness < completeness_threshold:
        failures.append(f"completeness below threshold: {completeness} < {completeness_threshold}")
    if dup_count > duplicate_threshold:
        failures.append(f"duplicate_title_count {dup_count} > {duplicate_threshold}")

    if failures:
        text = "Data Quality check failed:\n" + "\n".join(failures)
        payload = {"text": text}
        # send slack if available
        if slack_webhook_url:
            try:
                requests.post(slack_webhook_url, json=payload, timeout=5)
            except Exception:
                # don't mask original error, but continue to raise after
                pass
        raise Exception(text)
