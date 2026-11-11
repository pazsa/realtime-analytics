# Lab 03. FastAPI for order analytics

Add an API on top of the analytics from lab 02. Through FastAPI, serve the
total amount of paid orders, hourly sums with running totals, a list of
orders and a single order.

Everything runs in Docker Compose. Take your lab 02 environment and add the
API to it. Kafka, ClickHouse and event ingestion already exist; the lab 02
side-quests are not needed here.

For debugging, send the 14 events from the file below and compare the
responses. Keep the old data: it can be used to put some load on the API.

## Bring up the API

Add a FastAPI service to your Compose file. As usual, connect to ClickHouse
from the container by service name and internal port, for example
`clickhouse:8123`. Publish the API port for easier debugging; the examples
below use 18000.

Create the ClickHouse client once per process, for example in `lifespan`.
Configure timeouts and a limit on concurrent database queries. If a handler is
written with `async def`, do not block the event loop with a synchronous
client call. The driver and the way of limiting concurrency are up to you.

No caching yet. Amounts are returned as integer JSON numbers in cents, as in
lab 02.

The API reads the ready-made data from lab 02: the total from
`metrics_paid_amount`, the hourly sums from `metrics_paid_hourly`, and orders
from `check_current`. For `latest_event_at`, take the maximum `event_time`
from `check_raw`. The service adds HTTP filters, pagination and filling of
empty hours. Orders with `is_deleted=true` are excluded from order responses.

### Starter service

You can start from [lab03-api.py](lab03-api.py),
[lab03-pyproject.toml](lab03-pyproject.toml) and
[lab03-Dockerfile](lab03-Dockerfile). Put them next to your Compose file and
add the service:

```yaml
services:
  api:
    build:
      context: .
      dockerfile: lab03-Dockerfile
    environment:
      CLICKHOUSE_URL: http://clickhouse:8123
      CLICKHOUSE_DATABASE: lab02
      CLICKHOUSE_USER: <user with SELECT>
      CLICKHOUSE_PASSWORD: <password>
    ports:
      - "127.0.0.1:18000:8000"
```

Substitute your own ClickHouse service name, database and a user with SELECT
privileges. If you rename the files, fix the `Dockerfile` as well.

```bash
docker compose -f lab02-compose.yaml up -d --build api
curl 'http://127.0.0.1:18000/ping'
curl 'http://127.0.0.1:18000/example/orders?dataset_id=demo3&limit=5'
curl 'http://127.0.0.1:18000/example/orders-client?dataset_id=demo3&limit=5'
```

The starter creates two clients in `lifespan`: `httpx` for direct HTTP
requests and the async
[clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect). Both
use the ClickHouse HTTP interface. Bring ClickHouse up before the API:
clickhouse-connect checks the connection when the client is created.

`/example/orders` sends SQL through httpx and reads the database's JSON
response. `/example/orders-client` runs the same query through
`await ch_client.query()` and turns the result rows into a list of
dictionaries. SQL parameters are passed separately from the query text.
Timeouts and the concurrency limit are up to you. To check, first send the
events from the "Manual verification" section; for an empty dataset the
response is `{"data": []}`.

This is only a starting point. The endpoints below, their filters, pagination
and metadata are yours to implement. The example endpoints from the starter
are not part of the required interface.

### Availability check: /ping

`GET /ping` returns 200 and `{"status":"ok"}` without touching ClickHouse. It
must respond even when the database is unavailable.

Such an endpoint is sometimes called `/health` or `/healthz`. The name does not
define what it checks: some check only that the process is up, others also run
a test query such as `SELECT 1`. A check with a database query shows the API
can reach its dependency, but every call adds load, and a database outage can
make all such checks fail at once. A process-only check is cheap to call often.
In this lab `/ping` checks the process only.

## Endpoints

Keep these paths exactly:

```text
GET /ping
GET /v1/datasets/{dataset_id}/metrics/summary
GET /v1/datasets/{dataset_id}/metrics/timeseries
GET /v1/datasets/{dataset_id}/orders
GET /v1/datasets/{dataset_id}/orders/{order_id}
```

This is a JSON API. Responses contain JSON; GET parameters are passed in the
URL, through both the path and query params. `dataset_id` and `order_id` come
from the path; filters after `?` come from the query. For example:

```http
GET /v1/datasets/demo3/orders?status=paid&limit=2
```

Here `dataset_id=demo3`, `status=paid` and `limit=2`. No JSON body is needed.
The query parameters can be described with a Pydantic model, with
`extra="forbid"` to reject unknown query parameters. Since `from` is a Python
keyword, the time series field has to be named `from_` with
`Field(alias="from")`.

### Total amount

`GET /v1/datasets/{dataset_id}/metrics/summary` (takes no query parameters),
response 200:

```json
{
  "data": {"paid_amount_cents": 3501},
  "meta": {
    "computed_at": "2026-01-01T02:11:00.000Z",
    "latest_event_at": "2026-01-01T02:10:00.000Z"
  }
}
```

For an empty or unknown dataset the sum is 0.

### Hourly metrics

`GET /v1/datasets/{dataset_id}/metrics/timeseries` accepts:

- `from` and `to`: required dates with an explicit timezone, for example
  `2026-01-01T00:00:00Z`;
- `campaign_id`: optional campaign ID. If omitted, all campaigns are included.

Convert the boundaries to UTC: both must fall exactly on the start of an hour.
`from` must be less than `to`, and the range must be at most 31 days. The step
is one hour only. The interval is `[from, to)`: `from` is included, `to` is
not.

A 200 response contains `data` with rows of this form, and `meta`:

```json
{
  "data": [
    {
      "campaign_id": "beta",
      "bucket_start": "2026-01-01T02:00:00.000Z",
      "paid_amount_cents": 0,
      "cumulative_paid_amount_cents": 2501
    }
  ],
  "meta": {
    "computed_at": "2026-01-01T02:11:00.000Z",
    "latest_event_at": "2026-01-01T02:10:00.000Z"
  }
}
```

Return the paid sum for each hour and the running total of the campaign from
the start of the dataset up to and including that hour. As in lab 02, the
running total continues across day boundaries. If `from` starts later than the
first order, the sums of the earlier hours still count toward the running
total.

Return hours without paid orders with sum 0; the running total is preserved in
them. Before the first paid order it is 0.

With an explicitly chosen `campaign_id`, build the grid even for a campaign
that does not exist: all sums are 0. Without the filter, build the grid for
campaigns that have current non-deleted paid orders anywhere in the dataset,
including orders outside the range. If there are no such campaigns, `data` is
`[]`.

Sort rows by `campaign_id`, then by `bucket_start` ascending.

### List of orders

`GET /v1/datasets/{dataset_id}/orders` accepts:

- `status`: optional, one of created, paid or cancelled;
- `campaign_id`: optional campaign ID;
- `limit`: an integer from 1 to 100, 50 by default;
- `after_order_id`: optional ID after which to continue the listing.

Take current non-deleted orders, apply both filters, and sort by `order_id`
ascending in plain ASCII order. This is string sorting, so `o10` comes before
`o2`.

For the next page, pass the ID of the last order of the previous page as
`after_order_id`. In SQL, add the condition `order_id > after_order_id` so
that orders already returned do not appear again. The order with the given
`after_order_id` may no longer exist; this is not an error. The response must
contain at most `limit` orders.

```json
{
  "data": [
    {
      "order_id": "o1",
      "campaign_id": "beta",
      "version": 4,
      "status": "paid",
      "amount_cents": 1500,
      "created_at": "2026-01-01T00:59:59.999Z",
      "event_time": "2026-01-01T02:10:00.000Z"
    }
  ],
  "next_after_order_id": "o1"
}
```

All seven order fields are required; `version` and `amount_cents` are JSON
numbers. If there is a next page with the same filters, `next_after_order_id`
equals the ID of the last returned order. Otherwise it is null. For example,
you can request `limit + 1` rows to find out whether there is a continuation.

Request the next page with the same filters. While the data does not change,
paging must produce no gaps and no repeats. If orders change between
requests, an identical snapshot across all pages is not guaranteed. An empty
or unknown dataset gives `data: []` and `next_after_order_id: null`.

### Single order

`GET /v1/datasets/{dataset_id}/orders/{order_id}` returns 200 and the same
order object inside `data`, without pagination and without `meta`. A missing
or deleted order gives 404. Make sure the order belongs to the `dataset_id`.

## HTTP interface details

IDs in the path and query must match `[a-z0-9][a-z0-9_-]{0,63}`, as in lab 2.
Each request concerns only the `dataset_id` from the path.

Both metric responses include `meta`:

- `computed_at`: the time the result was computed, not the time of the last
  event;
- `latest_event_at`: the maximum `event_time` in the raw data of this dataset,
  including deletions and all statuses. For empty raw it is `null`.

Take `latest_event_at` over the whole dataset, even if the request is limited
by campaign or time. A recent date alone does not prove that the whole stream
has been processed. All dates in responses are UTC, with three millisecond
digits and `Z`.

An invalid ID, unknown status, wrong `limit` or date range, missing required
parameter, empty filter value and unknown query parameter all produce
[422](https://http.cat/422). For the integer `limit`, use integer notation,
for example 10, not 10.5. The error text and `detail` can stay as FastAPI
produces them; only the HTTP code matters.

If ClickHouse is unavailable or a query exceeds the timeout, return
[503](https://http.cat/503). Zeros in a successful response mean there are no
orders; they must not be used to mask a database error. If all query slots are
busy, [429](https://http.cat/429) or [503](https://http.cat/503) is acceptable
after a bounded wait. Pick limits and timeouts for your environment; there is
no RPS target.

## Manual verification

Ready-made data: [lab03-orders-example.jsonl](lab03-orders-example.jsonl).
Sender: [lab03-send-example.py](lab03-send-example.py). Put both files in the
same directory. The script uses Kafka at 127.0.0.1:19092 and the topic
`lab02.orders`; adjust them if your connections differ.

```bash
python3 -m pip install confluent-kafka==2.13.2
python3 lab03-send-example.py
```

The file has 14 publications, including duplicates and old versions, with
`dataset_id=demo3`. If needed, replace `dataset_id` in all lines with a new
one and use it in all requests. Re-sending the file must not change the API
responses.

Wait for processing and check:

```bash
curl 'http://127.0.0.1:18000/v1/datasets/demo3/metrics/summary'
curl 'http://127.0.0.1:18000/v1/datasets/demo3/orders?limit=2'
curl 'http://127.0.0.1:18000/v1/datasets/demo3/orders/o1'
curl -i 'http://127.0.0.1:18000/v1/datasets/demo3/orders/o3'
```

Expected results:

- summary: `paid_amount_cents=3501`, `latest_event_at=2026-01-01T02:10:00.000Z`.
  `computed_at` depends on when your request runs.
- First page: o1 and o2, `next_after_order_id=o2`.
- With `after_order_id=o2`: o4 and o5, continuation o5.
- With `after_order_id=o5`: o6 and o7, no continuation.
- With `status=paid`: o1, o4, o5, o7. With `status=paid&campaign_id=alpha`:
  only o7.
- Card of o1: version 4, campaign beta, paid, amount 1500.
- Card of o3: 404. An old version after the deletion does not bring the order
  back.

For the time series:

```bash
curl 'http://127.0.0.1:18000/v1/datasets/demo3/metrics/timeseries?from=2026-01-01T00:00:00Z&to=2026-01-01T03:00:00Z'
```

All hours below are on 1 January 2026, UTC:

| Campaign | Hour  | Sum for the hour | Running total |
|----------|-------|------------------|---------------|
| alpha    | 00:00 | 0                | 0             |
| alpha    | 01:00 | 1000             | 1000          |
| alpha    | 02:00 | 0                | 1000          |
| beta     | 00:00 | 1500             | 1500          |
| beta     | 01:00 | 1001             | 2501          |
| beta     | 02:00 | 0                | 2501          |

With `campaign_id=beta`, three beta rows remain. With `from=01:00` and
`to=03:00` in the full date format, the hours 01:00 and 02:00 remain, and the
beta running total is still 2501. The 03:00 hour is not in the response.

### Corrections and duplicates

Re-send the original file: the sum and responses must stay the same. Then
change the file one step at a time and send it with the same script:

1. In the first line, set for o1 `version=5`, `event_id=e_o1_v5`,
   `status=paid`, `campaign_id=alpha` and `amount_cents=2000`. Keep the other
   fields. Expected sum: 4001; the o1 card moves to alpha. The alpha total
   becomes 3000, beta becomes 1001.
2. Return the first line to its original version and send the file again.
   This is an old event: the responses must not change.
3. In the o7 line, set `version=2`, `event_id=e_o7_v2` and `is_deleted=true`.
   Keep the other fields. The sum becomes 3001, o7 disappears from the list,
   and its card returns 404.

Also try `limit=0`, `status=paidd`, dates without a timezone, the boundary
00:30, or `from >= to`: 422 is expected. For a missing campaign, the time
series contains zero hours. For a new empty dataset, summary is 0,
`latest_event_at` is null, and the order list and the series without a
campaign filter are empty.
