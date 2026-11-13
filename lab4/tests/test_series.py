from app import fill_series

HOUR = 3600


def summarize(points):
    return [(p.campaign_id, p.paid_amount_cents, p.cumulative_paid_amount_cents) for p in points]


def test_empty_hours_are_filled_and_total_carries_forward():
    start = 10 * HOUR
    rows = [
        ("c1", 10 * HOUR, 100, 100),
        ("c1", 12 * HOUR, 50, 150),
    ]
    out = fill_series(rows, start, 4, None)
    assert summarize(out) == [
        ("c1", 100, 100),
        ("c1", 0, 100),
        ("c1", 50, 150),
        ("c1", 0, 150),
    ]


def test_total_before_the_range_is_the_starting_point():
    rows = [("c1", 8 * HOUR, 70, 70)]
    out = fill_series(rows, 10 * HOUR, 2, None)
    assert summarize(out) == [("c1", 0, 70), ("c1", 0, 70)]


def test_requested_campaign_without_data_returns_zeros():
    out = fill_series([], 10 * HOUR, 2, "c9")
    assert summarize(out) == [("c9", 0, 0), ("c9", 0, 0)]


def test_campaigns_are_returned_in_sorted_order():
    rows = [
        ("b", 10 * HOUR, 5, 5),
        ("a", 10 * HOUR, 7, 7),
    ]
    out = fill_series(rows, 10 * HOUR, 1, None)
    assert [p.campaign_id for p in out] == ["a", "b"]


def test_bucket_start_is_iso_utc_with_milliseconds():
    out = fill_series([], 10 * HOUR, 1, "c1")
    assert out[0].bucket_start == "1970-01-01T10:00:00.000Z"
