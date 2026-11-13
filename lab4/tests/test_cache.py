from unittest.mock import patch

import pytest

from cache import TTLCache


def test_put_then_get_returns_value():
    c = TTLCache(ttl=30, max_entries=8)
    c.put("a", {"x": 1})
    assert c.get("a") == {"x": 1}


def test_missing_key_returns_none():
    assert TTLCache().get("nope") is None


def test_entry_expires_after_ttl():
    with patch("cache.monotonic") as clock:
        clock.return_value = 100.0
        c = TTLCache(ttl=30, max_entries=8)
        c.put("a", 1)

        clock.return_value = 129.9
        assert c.get("a") == 1  # still fresh

        clock.return_value = 130.0
        assert c.get("a") is None  # expired exactly at the TTL
        assert "a" not in c.entries  # and removed from the cache


def test_oldest_entry_is_evicted_when_full():
    c = TTLCache(ttl=30, max_entries=2)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    assert c.get("a") is None
    assert c.get("b") == 2
    assert c.get("c") == 3


def test_putting_an_existing_key_refreshes_it():
    c = TTLCache(ttl=30, max_entries=2)
    c.put("a", 1)
    c.put("b", 2)
    c.put("a", 10)  # "a" is now the newest, so "b" is the oldest
    c.put("c", 3)
    assert c.get("b") is None
    assert c.get("a") == 10


def test_invalid_arguments_raise():
    with pytest.raises(ValueError):
        TTLCache(ttl=0, max_entries=8)
    with pytest.raises(ValueError):
        TTLCache(ttl=-1, max_entries=8)
    with pytest.raises(ValueError):
        TTLCache(ttl=30, max_entries=0)
