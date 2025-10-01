from dags.analysis import parse_genres, analyze_movies_from_list


def test_parse_genres_variants():
    assert parse_genres(None) == []
    assert parse_genres(["Action", "Adventure"]) == ["Action", "Adventure"]
    assert parse_genres("[\"Action\", \"Drama\"]") == ["Action", "Drama"]
    assert parse_genres("Action, Drama") == ["Action", "Drama"]
    assert parse_genres("['Action','Drama']") == ["Action", "Drama"]


def test_analyze_movies_basic():
    movies = [
        {"title": "Movie A", "year": 2000, "genres": ["Action"]},
        {"title": "Movie B", "year": 2001, "genres": "Action, Drama"},
        {"title": None, "year": None, "genres": None},
    ]
    res = analyze_movies_from_list(movies)
    assert res["total_movies"] == 3
    assert res["movies_by_year"][2000] == 1
    assert res["genre_counts"]["Action"] >= 2
    assert res["missing_title_count"] == 1
    # new metrics
    assert "top_genre_pairs" in res
    assert isinstance(res["top_genre_pairs"], list)
    assert "movies_by_year_trend" in res


def test_recommendations_and_quality():
    movies = [
        {"title": "A", "year": 2000, "genres": ["Action", "Adventure"]},
        {"title": "B", "year": 2001, "genres": ["Action"]},
        {"title": "C", "year": 2002, "genres": ["Drama"]},
        {"title": "D", "year": 2002, "genres": None},
        {"title": "A", "year": 2000, "genres": ["Action"]},
    ]

    from dags.analysis import compute_genre_similarity_recommendations, data_quality_report

    recs = compute_genre_similarity_recommendations(movies, top_n=2)
    assert isinstance(recs, dict)
    assert "A" in recs
    assert isinstance(recs["A"], list)

    dq = data_quality_report(movies)
    assert dq["total_rows"] == 5
    assert "duplicate_title_count" in dq


def test_check_and_alert_dq(monkeypatch):
    from dags.alerts import check_and_alert_dq

    called = {}

    def fake_post(url, json=None, timeout=5):
        called['url'] = url
        called['payload'] = json
        class R: pass
        return R()

    # Good report - should not raise
    good_report = {"completeness": 0.95, "duplicate_title_count": 0}
    check_and_alert_dq(good_report, slack_webhook_url=None)

    # Bad report - should call slack and raise
    bad_report = {"completeness": 0.5, "duplicate_title_count": 10}
    monkeypatch.setattr('requests.post', fake_post)
    try:
        check_and_alert_dq(bad_report, slack_webhook_url='http://slack/webhook', completeness_threshold=0.8, duplicate_threshold=5)
        raised = False
    except Exception as e:
        raised = True

    assert raised
    assert called.get('url') == 'http://slack/webhook'
