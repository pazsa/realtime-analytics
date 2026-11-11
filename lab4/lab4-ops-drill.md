# Lab 04. Metrics, alerts and cache

Add monitoring to the API from lab 03. Collect request counts, response times
and errors in Prometheus. Set up alerts for API unavailability and for data
read errors.

A cache is added for the summary calculation. While it is valid, the service
can return the last computed response even without ClickHouse. The calculation
time is already in `meta.computed_at`; when a response is read from the cache,
it must stay unchanged.

Everything runs in Docker Compose. Take your lab 03 environment and add
Prometheus. Then shut down ClickHouse, check the API responses and the alerts
before and after the TTL expires, and restore the database.

## Service metrics

Take the API from lab 03. The starter files below add Prometheus to your
Compose setup and configure metrics collection from the API.

### Connect Prometheus

Starter files to put next to your Compose file:

- [lab04-compose.yaml](lab04-compose.yaml);
- [lab04-prometheus.yml](lab04-prometheus.yml);
- [lab04-alerts.yml](lab04-alerts.yml).

In the example, the API is called `api` and listens on port 8000 inside the
container. If your names or ports differ, change the target in
`lab04-prometheus.yml`.

```bash
docker compose -f <your-compose>.yaml -f lab04-compose.yaml up -d prometheus
curl 'http://127.0.0.1:18000/metrics'
```

Open Prometheus in the browser at http://127.0.0.1:19004. In the Targets
section, the `api` job must be UP. Prometheus is configured to scrape metrics
every 2s.

### Add a cache for summary

Use [lab04-cache.py](lab04-cache.py) as a reference: it contains a dictionary
with a TTL and a limit on the number of entries. If you run the service
without Docker, rename the module to `cache.py`; in Docker you can
`COPY lab04-cache.py cache.py` in the Dockerfile and then use
`from cache import TTLCache` in Python.

With FastAPI, wire the cache like this: create `TTLCache(ttl=30,
max_entries=128)` once in `lifespan` and keep it in `app.state`.

- `get(dataset_id)` returns a response or None;
- `put(dataset_id, response)` stores a response and restarts the TTL.

Connect this cache to `GET /v1/datasets/{dataset_id}/metrics/summary`: it
keeps a response for 30 seconds. For this lab an in-process dictionary in the
API is enough: one worker, keyed by `dataset_id`, at most 128 entries.

Store the whole successful response, including `data` and `meta`. When
reading from the cache, do not change `computed_at` or `latest_event_at`, and
do not extend the TTL. When the cache is full, evict the oldest entry.

- The entry is still valid: return it without querying ClickHouse.
- There is no entry or the TTL has expired: read ClickHouse and store the new
  response.
- Reading the database fails: return 503.

Do not cache errors. And do not return an expired response in place of an
error.

With the query parameter `refresh=true`, read the database regardless of what
is in the cache. On a successful read, update the cached response (together
with its TTL). On an error, return 503 and leave the valid cache entry
unchanged. Without the parameter, or with `refresh=false`, use the normal
cache behavior. This makes it possible to compare a cached response with the
result of a database query.

The same behavior could be requested with the `Cache-Control: no-cache`
header. This lab uses a query parameter because it is easier to add to a URL
in a browser or with curl. Header support is left as an exercise.

Do not cache the other endpoints yet. `/ping` and `/metrics` must respond even
when ClickHouse is unavailable. The cache is empty after an API restart; that
is fine.

### Add API metrics

Add `GET /metrics` and serve Prometheus metrics through it. You can use
`prometheus_client`. Three metrics are required:

- `lab04_http_requests_total`: a Counter with labels `route` and `status`.
  Counts completed HTTP requests, including errors.
- `lab04_http_duration_seconds`: a Histogram with label `route`. Measures
  response time in seconds. Includes cache reads, database queries and error
  responses.
- `lab04_cache_requests_total`: a Counter with label `result`: `hit` or
  `miss`. Count `hit` when serving a response from a valid cache; `miss` when
  the database has to be read. A `refresh=true` request and a failed database
  read also count as `miss`.

In `route`, use the route template
`/v1/datasets/{dataset_id}/metrics/summary`, not the address of a specific
dataset. For an unknown route use `unmatched`. Pass the HTTP code as a string
in `status`, for example `"200"` or `"503"`.

Do not add `dataset_id`, `order_id` or `request_id` to labels: it bloats the
metrics, and every new ID creates new time series. Exclude `/metrics` itself
from the request counter and the histogram, otherwise Prometheus's own
requests end up in your API graphs.

Suitable histogram buckets are 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2 and 5 seconds.
Increase counters with `inc()` and record response time with `observe()`.

### Inspect metrics in Prometheus

Make several summary requests and find in Prometheus:

```promql
lab04_http_requests_total
lab04_cache_requests_total
```

Look at the request rate:

```promql
sum(rate(lab04_http_requests_total[30s]))
```

And the p95 response time of summary:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(lab04_http_duration_seconds_bucket{
      route="/v1/datasets/{dataset_id}/metrics/summary"
    }[30s])
  )
)
```

If the result or graph is empty, make a few more requests and wait for several
scrapes: `rate` and p95 need an observation interval.

### Configure alerts

`lab04-alerts.yml` already contains `ApiUnavailable`: `up{job="api"} == 0`
with `for: 10s`. It fires when Prometheus cannot scrape the API metrics. When
ClickHouse is stopped, `/metrics` keeps responding, so this alert must stay
inactive.

Add an `ApiErrors` rule:

- compute the share of 5xx among summary responses over 30s;
- the alert must fire when the share is above 50%;
- add `for: 10s` so a single spike does not trigger it;
- in `annotations.summary` say what broke, and in `description` say which
  query or dependency to check first;
- when there are no requests, this alert must not fire.

See the Prometheus
[alerting rules documentation](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/).

Use the rate of the counter filtered by `route` and `status`, then divide the
rate of 5xx by the rate of all summary responses. Use the fixed route template
so that successful responses of other endpoints do not hide summary errors.

The window and `for` are short here to avoid waiting long for the alert. After
every rule change, restart Prometheus:

```bash
docker compose -f lab02-compose.yaml -f lab04-compose.yaml restart prometheus
```

## Shut down ClickHouse and see what happens

You need a dataset; for example the one from lab 03. In the commands below it
is `demo3`, and the services are `api` and `clickhouse`. If your names
differ, substitute your own.

First read summary directly and warm up the cache:

```bash
curl -i \
  'http://127.0.0.1:18000/v1/datasets/demo3/metrics/summary?refresh=true'
```

Save the sum, `computed_at` and `latest_event_at`. Immediately stop the
database:

```bash
docker compose -f lab02-compose.yaml stop clickhouse
```

Repeat the plain summary request until the entry expires. You can watch the
responses once per second right in the shell:

```bash
while true; do
  curl -i 'http://127.0.0.1:18000/v1/datasets/demo3/metrics/summary'
  sleep 1
done
```

Stop the loop with Ctrl+C when you've seen enough. Expected behavior:

1. Before the TTL expires, summary returns 200, the same data and the same
   `computed_at`.
2. A `refresh=true` request already returns 503.
3. After the TTL expires, the plain summary also returns 503.
4. `/ping` and `/metrics` keep responding with 200, and the `api` target stays
   UP.
5. If you keep making requests that fail, `ApiErrors` goes through pending and
   firing. `ApiUnavailable` stays inactive.

Restore ClickHouse:

```bash
docker compose -f lab02-compose.yaml start clickhouse
```

Wait for the database to be ready and request summary again. It must return
200 and a new `computed_at`. If there were no new events, `latest_event_at`
and the sum may stay the same: you recomputed the response, but received no
new events.

Keep making successful requests and wait for `ApiErrors` to return to
inactive. The alert goes off later than the successful responses come back:
that is expected, since its window still includes the earlier errors.

Now stop the API itself:

```bash
docker compose -f lab02-compose.yaml stop api
```

Wait for `ApiUnavailable` to fire. Start the API and check that the target is
UP again and the alert is back to inactive:

```bash
docker compose -f lab02-compose.yaml start api
```

## Expected behavior

- The cache separates datasets and keeps `data` and `meta` until the TTL
  expires. `refresh` reads fresh data; a plain read does not extend the TTL.
- Metrics are collected in Prometheus: hit/miss, requests by route template
  and response code, and a latency histogram. `/metrics` itself is not
  counted.
- With the database down, `refresh` gives 503, a valid cache entry still gives
  200, and after the TTL it also gives 503. `/ping` and `/metrics` keep
  responding.
- `ApiErrors` fires on summary errors and clears after recovery.
  `ApiUnavailable` fires when the API is stopped and clears after it starts.
- After recovery, summary again returns the correct sum and a new
  `computed_at`.
