import json
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from collections import Counter
import numpy as np


def parse_genres(genres: Any) -> List[str]:
    """Normalize the genres field into a list of strings.

    Accepts lists, JSON-encoded lists, bracketed strings, or comma-separated strings.
    Returns an empty list for missing/None values.
    """
    if genres is None:
        return []
    if isinstance(genres, (list, tuple)):
        return [str(g).strip() for g in genres if g is not None]
    if isinstance(genres, str):
        s = genres.strip()
        # Try JSON parse
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(g).strip() for g in parsed if g is not None]
        except Exception:
            pass
        # Strip surrounding brackets and split on commas
        s2 = s.strip("[] ")
        if not s2:
            return []
        return [p.strip().strip("'\"") for p in s2.split(",") if p.strip()]
    # Fallback
    return [str(genres)]


def analyze_movies_from_list(movies: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute simple analytics from a list of movie dicts.

    Returns a dict with total count, movies_by_year, genre_counts, top_titles, and missing counts.
    """
    if not isinstance(movies, list):
        raise ValueError("movies must be a list of dicts")

    df = pd.DataFrame(movies)

    # Defensive columns
    for col in ["title", "year", "genres"]:
        if col not in df.columns:
            df[col] = None

    if df.empty:
        return {
            "total_movies": 0,
            "movies_by_year": {},
            "genre_counts": {},
            "top_titles": {},
            "missing_title_count": 0,
            "missing_year_count": 0,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }

    # Normalize genres
    df["genres_list"] = df["genres"].apply(parse_genres)

    total_movies = int(len(df))
    movies_by_year = df.groupby(df["year"]).size().dropna().to_dict()

    genres_exploded = df.explode("genres_list")
    genre_counts = (
        genres_exploded["genres_list"].value_counts(dropna=True).to_dict()
    )

    # Co-genre pairs (unordered) - count how often two genres appear together
    pair_counts = Counter()
    for gen_list in df["genres_list"]:
        unique_g = sorted(set([g for g in gen_list if g]))
        for i in range(len(unique_g)):
            for j in range(i + 1, len(unique_g)):
                pair = (unique_g[i], unique_g[j])
                pair_counts[pair] += 1

    top_genre_pairs = [ {"pair": list(k), "count": v} for k, v in pair_counts.most_common(10) ]

    # Year trend: movies per year sorted by year
    movies_by_year_series = {
        int(k): int(v) for k, v in sorted(movies_by_year.items(), key=lambda x: (x[0] if pd.notna(x[0]) else 0))
    }

    top_titles = df["title"].value_counts(dropna=True).head(10).to_dict()

    missing_title_count = int(df["title"].isna().sum())
    missing_year_count = int(df["year"].isna().sum())

    # Convert keys that may be numpy types to native Python types
    movies_by_year_native = {int(k): int(v) for k, v in movies_by_year.items() if pd.notna(k)}

    analysis = {
        "total_movies": total_movies,
        "movies_by_year": movies_by_year_native,
        "genre_counts": {str(k): int(v) for k, v in genre_counts.items()},
        "top_titles": {str(k): int(v) for k, v in top_titles.items()},
        "missing_title_count": missing_title_count,
        "missing_year_count": missing_year_count,
        "top_genre_pairs": top_genre_pairs,
        "movies_by_year_trend": movies_by_year_series,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }

    return analysis


def compute_genre_similarity_recommendations(movies: List[Dict[str, Any]], top_n: int = 5) -> Dict[str, List[Dict[str, Any]]]:
    """Compute simple genre-based recommendations using cosine similarity on one-hot genre vectors.

    Returns a mapping {movie_title: [{title: other_title, score: float}, ...]}
    """
    if not movies:
        return {}

    df = pd.DataFrame(movies)
    for col in ["title", "genres"]:
        if col not in df.columns:
            df[col] = None

    # Normalize genres into lists
    df["genres_list"] = df["genres"].apply(parse_genres)

    # Build a unique genre list and binary vectors
    all_genres = sorted({g for lst in df["genres_list"] for g in lst if g})
    if not all_genres:
        return {row["title"]: [] for _, row in df.iterrows()}

    genre_index = {g: i for i, g in enumerate(all_genres)}
    mat = np.zeros((len(df), len(all_genres)), dtype=float)
    for i, lst in enumerate(df["genres_list"]):
        for g in lst:
            if g in genre_index:
                mat[i, genre_index[g]] = 1.0

    # Normalize vectors to avoid zero-division; if zero vector, leave as zeros
    norms = np.linalg.norm(mat, axis=1)
    norms[norms == 0] = 1.0
    mat_norm = mat / norms[:, None]

    # Cosine similarity
    sim = mat_norm.dot(mat_norm.T)

    recommendations = {}
    titles = df["title"].fillna("").tolist()
    for i, title in enumerate(titles):
        row_sim = sim[i]
        # Exclude self
        pairs = [(j, float(row_sim[j])) for j in range(len(titles)) if j != i]
        pairs.sort(key=lambda x: x[1], reverse=True)
        top = []
        for j, score in pairs[:top_n]:
            top.append({"title": titles[j], "score": round(score, 4)})
        recommendations[title or f"_no_title_{i}"] = top

    return recommendations


def data_quality_report(movies: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Produce a data quality report: counts, nulls, duplicates, and simple anomaly checks."""
    df = pd.DataFrame(movies)
    report = {}
    report["total_rows"] = int(len(df))
    report["null_counts"] = df.isna().sum().to_dict()

    # Duplicate titles
    if "title" in df.columns:
        dup_titles = df["title"].duplicated(keep=False)
        report["duplicate_title_count"] = int(dup_titles.sum())
        report["duplicate_titles_sample"] = df.loc[dup_titles, "title"].dropna().unique().tolist()[:10]
    else:
        report["duplicate_title_count"] = 0
        report["duplicate_titles_sample"] = []

    # Year anomalies: non-numeric or out-of-range
    if "year" in df.columns:
        years = pd.to_numeric(df["year"], errors="coerce")
        report["year_null_count"] = int(years.isna().sum())
        if years.dropna().empty:
            report["year_range"] = None
        else:
            report["year_range"] = {"min": int(years.min()), "max": int(years.max())}
    else:
        report["year_null_count"] = 0
        report["year_range"] = None

    # Basic completeness score: proportion of rows with title and year
    has_title = df.get("title") is not None and df["title"].notna()
    has_year = df.get("year") is not None and df["year"].notna()
    completeness = 0.0
    if len(df) > 0:
        completeness = float(((df["title"].notna() & df["year"].notna()).sum()) / len(df))
    report["completeness"] = round(completeness, 4)

    return report


def analyze_movies_from_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        movies = json.load(f)
    return analyze_movies_from_list(movies)


if __name__ == "__main__":
    # Simple CLI for local testing; expects ./tests/sample_movies.json
    import sys

    sample = "tests/sample_movies.json"
    if len(sys.argv) > 1:
        sample = sys.argv[1]
    try:
        out = analyze_movies_from_json_file(sample)
        print(json.dumps(out, indent=2))
    except Exception as e:
        print(f"Error running analysis: {e}")
        raise
