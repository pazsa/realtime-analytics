# Lab 02. ClickHouse and a change stream

Build an order analytics service. Consume changes from Kafka, keep the full
history, and expose up-to-date totals in ClickHouse. Orders can change their
status, amount and ad campaign. Events can be duplicated and can arrive late.
Metrics must reflect the latest state of each order.

Two calculations are required:

- the total amount of current paid orders;
- the hourly amount per campaign, with a running total within each campaign.

Everything runs in Docker Compose. A later lab adds a FastAPI service on top of
the same data, so keep the environment and the data.

You bring up Kafka and ClickHouse, set up event ingestion, and write the SQL
for history, current state and the two metrics. The 14-event example below can
be sent with a ready-made script to debug the solution.

## Before you start

Tested versions: Kafka 4.2.1, ClickHouse 25.8. The work builds on earlier
topics: delivery duplicates, the versioning model, Kafka Engine and
materialized views.

Start with the event format and the 14-publication example below. That set is
enough to debug the core logic. The solution should also handle a much larger
load: 200 thousand orders and one million publications, with 32 campaigns,
28 days, duplicates and several versions of the same order. It is not
practical to debug against that full load.

## Event format

Each Kafka record contains one UTF-8 JSON object with the full state of an
order. All fields are required and cannot be null. The order of JSON fields
does not matter; duplicate field names are not allowed. There is no Debezium
envelope in this base mode.

| field        | type | allowed values         | meaning                           |
|--------------|------|------------------------|-----------------------------------|
| dataset_id   | str  | ID format below        | dataset identifier                |
| event_id     | str  | ID format              | event identifier within a dataset |
| order_id     | str  | ID format              | order identifier                  |
| campaign_id  | str  | ID format              | current ad campaign               |
| version      | int  | 1 to 2147483647        | version of the order state        |
| status       | str  | created\|paid\|cancelled | current status                  |
| amount_cents | int  | 0 to 1000000000        | amount (in cents)                 |
| is_deleted   | bool |                        | logical deletion                  |
| created_at   | str  | time format below      | order creation time               |
| event_time   | str  | time format below      | time of the change at the source  |

An ID matches `[a-z0-9][a-z0-9_-]{0,63}`: 1 to 64 ASCII characters, lowercase
letters, digits, hyphen and underscore only; the first character is a letter
or a digit. Numbers are sent as plain JSON integers: no quotes, fractions or
exponent notation. `is_deleted` is `true`/`false`, not `0`/`1`. All amounts
are in one conventional currency; no currency conversion is needed.

Time has the format `YYYY-MM-DDTHH:MM:SS.sssZ`: UTC with three millisecond
digits, no local offsets, no leap seconds. The allowed range is
`[2020-01-01T00:00:00.000Z, 2100-01-01T00:00:00.000Z)`. For every order,
`event_time >= created_at`. As `version` grows, the change time does not
decrease, but it may be equal across different versions.

The Kafka key holds the logical order key `(dataset_id, order_id)` as UTF-8
JSON with a fixed order of the two fields:

```json
{"dataset_id":"demo","order_id":"o1"}
```

The same key is kept for all versions and duplicates of an order. Example
value:

```json
{
  "dataset_id": "demo",
  "event_id": "e_o1_v4",
  "order_id": "o1",
  "campaign_id": "beta",
  "version": 4,
  "status": "paid",
  "amount_cents": 1500,
  "is_deleted": false,
  "created_at": "2026-01-01T00:59:59.999Z",
  "event_time": "2026-01-01T02:10:00.000Z"
}
```

### Rules for versions, duplicates and deletion

- `dataset_id`, `order_id` and `created_at` never change across the versions
  of an order.
- Each new state gets the next `version` and a new `event_id`. Gaps in
  versions are allowed; the first version received is not necessarily 1.
- Within a `dataset_id`, one `event_id` corresponds to one state of one
  order. A duplicate preserves all event fields, including `event_id` and
  `version`.
- For `(dataset_id, order_id, version)` there is exactly one state and one
  `event_id`. There are no conflicting states for the same version.
- A new version may change `status`, `amount_cents` and `campaign_id`.
  `paid` -> `cancelled` is allowed, as are increases and decreases of the
  amount.
- Deletion arrives as a full state with a greater `version` and
  `is_deleted=true`. The other business fields are preserved. There are no
  new live versions after a deletion.
- Old live events stay in raw after a deletion and do not bring the order
  back into the metrics. The deletion version may arrive before the other
  versions of the order.

Kafka preserves publication order within a partition, but versions may be
published in the order `v4` -> `v3`. Pick the state by the maximum `version`;
neither the Kafka offset, nor `event_time`, nor the ingestion time can replace
it.

## Example: 14 publications

Ready-made events: [lab02-orders-example.jsonl](lab02-orders-example.jsonl).
Each line of the file is one full JSON object; duplicates are already
included.

All records belong to `dataset_id=demo`. For all versions, `event_time` equals
`2026-01-01T02:10:00.000Z`: this timestamp intentionally does not help pick
the latest version. In the example, `event_id` has the form
`e_{order_id}_v{version}`; this convention is for the example only, and
`event_id` must not be parsed to choose a version.

| order_id | created_at for all versions of the order |
|----------|------------------------------------------|
| o1       | 2026-01-01T00:59:59.999Z                 |
| o2       | 2026-01-01T00:10:00.000Z                 |
| o3       | 2026-01-01T01:00:00.000Z                 |
| o4       | 2026-01-01T01:00:00.000Z                 |
| o5       | 2026-01-01T01:30:00.000Z                 |
| o6       | 2026-01-01T01:45:00.000Z                 |
| o7       | 2026-01-01T01:59:00.000Z                 |

Each line is a full JSON object with `dataset_id`, `event_id` and dates
following the rules above.

|  # | order_id | version | campaign_id |    status | amount_cents | is_deleted | note                               |
|----|----------|---------|-------------|-----------|-------------:|------------|------------------------------------|
|  1 |       o1 |       1 |       alpha |   created | 1000         | false      | creation                           |
|  2 |       o1 |       2 |       alpha |      paid | 1000         | false      | payment                            |
|  3 |       o1 |       2 |       alpha |      paid | 1000         | false      | duplicate of #2                    |
|  4 |       o1 |       4 |        beta |      paid | 1500         | false      | amount/campaign correction         |
|  5 |       o1 |       3 |       alpha |      paid | 1200         | false      | late version                       |
|  6 |       o2 |       1 |       alpha |      paid | 2000         | false      | full paid state                    |
|  7 |       o2 |       2 |       alpha | cancelled | 2000         | false      | cancellation                       |
|  8 |       o3 |       1 |       alpha |      paid | 3000         | false      | full paid state                    |
|  9 |       o3 |       2 |       alpha |      paid | 3000         | true       | deletion                           |
| 10 |       o3 |       1 |       alpha |      paid | 3000         | false      | duplicate of #8, after the deletion |
| 11 |       o4 |       1 |        beta |      paid | 0            | false      | zero amount                        |
| 12 |       o5 |       1 |        beta |      paid | 1001         | false      | paid order                         |
| 13 |       o6 |       1 |       alpha |   created | 500          | false      | unpaid order                       |
| 14 |       o7 |       1 |       alpha |      paid | 1000         | false      | paid order                         |

## 1. Bring up the environment and ingestion

Build the pipeline:

```text
producer -> Kafka -> Kafka Engine + MV -> raw -> current -> metrics
```

Use the starter [lab02-compose.yaml](lab02-compose.yaml) and run it from the
same directory:

```bash
docker compose -f lab02-compose.yaml up -d --wait
```

The starter brings up Kafka and ClickHouse, creates the topic `lab02.orders`
with three partitions, and keeps data in Docker volumes. Kafka is available at
`127.0.0.1:19092`, ClickHouse over HTTP at `127.0.0.1:18132`. Inside the
Docker network the addresses are different: `kafka:9092` and
`clickhouse:8123`.

The starter contains no SQL. Create the database `lab02` and your own tables
and views. To run SQL, open the client:

```bash
docker compose -f lab02-compose.yaml exec clickhouse clickhouse-client --user lab02_admin --password lab02_local
```

For a new data set there is no need to recreate the topic or reset offsets.
A plain `docker compose -f lab02-compose.yaml down` keeps the data;
`down -v` removes the volumes.

Use Kafka Engine and a materialized view to ingest events into raw. The
engines of the internal tables, the sorting keys and the way current is built
are up to you. If you completed the first lab, you can reuse that
environment, but bring the schema in line with the event format above.

For each attempt, choose a new `dataset_id` and use it in all publications
and queries. Identical `order_id` values from different datasets must be
independent. The queries below use `dataset_id=demo`; when testing on your
own, replace it with your `dataset_id`.

## 2. Keep the history and build current

Prepare two SQL entities (a view or a table), available through SELECT:

- `check_raw` returns all received events, including every repeated delivery;
- `check_current` returns one row with the maximum version for each
  `(dataset_id, order_id)` pair, **including** deleted orders.

Send the 14 publications with
[lab02-send-example.py](lab02-send-example.py). Put the script and
`lab02-orders-example.jsonl` in the same directory, install the library and
run the script:

```bash
python3 -m pip install confluent-kafka==2.13.2
python3 lab02-send-example.py
```

By default the script connects to 127.0.0.1:19092 and the topic
`lab02.orders`. For a different connection, edit the corresponding variables
in the script.

The script builds the Kafka key from `dataset_id` and `order_id`, sends the
lines in file order and waits for Kafka to acknowledge them. To test
duplicates, run it again. For another dataset, replace `dataset_id` in all
lines of the file.

`check_raw` must contain at least 14 rows and exactly 12 distinct `event_id`
values. Extra exact copies from repeated delivery are allowed.
`check_current` must hold 7 orders, one of them deleted.

Check two orders separately. For `o1`, `v4` must remain, with campaign `beta`
and amount 1500, even though `v3` arrived later. For `o3`, the version with
`is_deleted=true` must remain, despite the repeat of an old live event.

Select the latest state before filtering by status, campaign and deletion.
For example, if you keep only paid first, you may lose a newer cancellation
and bring back an old payment. The answer must be correct before background
merges complete. Running OPTIMIZE or recomputing manually between changes does
not solve this task.

## 3. Total paid amount

Create a parameterized view `metrics_paid_amount`. For the chosen
`dataset_id`, sum `amount_cents` over current, non-deleted orders with
`status='paid'`. Return one row; if there are no matching orders, the sum is 0.

```sql
SELECT * FROM lab02.metrics_paid_amount(p_dataset_id = 'demo');
```

Notes: there is no `GROUP BY` here, because one row is required even when no
orders match. In that case `sum` returns 0 and `dataset_id` comes from the
parameter. A query with grouping would return an empty result instead. See the
ClickHouse documentation on
[parameterized views](https://clickhouse.com/docs/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views).

After the first five publications of the example, the sum must change like
this:

```text
0 -> 1000 -> 1000 -> 1500 -> 1500
```

After all 14 publications, `3501` is expected. Replay the whole batch: raw
grows, but current and the sum must stay the same.

## 4. Hourly sum with a running total

Create a view `metrics_paid_hourly`. Take the same current paid orders, group
them by `campaign_id` and by the hour of `created_at` in UTC. For each row,
return the sum for that hour and the sum of that campaign from the first hour
of the dataset up to and including the current one. The running total
continues across day boundaries.

Group by the order creation time. If an order is created at 00:59 and paid at
02:10, its amount **belongs to the 00:00 hour**. The calculation shows the
current state of orders by creation time, not the inflow of money by payment
time.

```sql
SELECT * FROM lab02.metrics_paid_hourly(p_dataset_id = 'demo')
ORDER BY campaign_id, bucket_start;
```

For the 14-publication example, three rows are expected. All hours below are
on 1 January 2026, UTC:

| campaign_id | bucket_start | paid_amount_cents | cumulative_paid_amount_cents |
|-------------|--------------|------------------:|-----------------------------:|
|       alpha | 01:00        |              1000 |                         1000 |
|        beta | 00:00        |              1500 |                         1500 |
|        beta | 01:00        |              1001 |                         2501 |

There is no alpha row for 00:00: `o2` is cancelled and `o1` moved to beta.
Skip hours without paid orders. A paid order with a zero amount, on the
contrary, produces a row with sum 0. For an empty dataset this view returns an
empty result.

A window function is needed on top of the grouping: `GROUP BY` leaves one row
per campaign and hour, and the window adds a running total to each row. Use
`PARTITION BY campaign_id` to start a separate accumulation per campaign,
`ORDER BY bucket_start` for the hour order, and a frame from the start of the
campaign to the current row. Apply the window to the already computed hourly
sums.

When debugging on your own, you can modify an old order: decrease the amount,
move it to another campaign, or cancel it. Send the full new state with the
next version and a new `event_id`. Check its hour and the running totals of
the following hours. An ordinary view over current can compute them at read
time. If you accumulate every incoming version through an incremental MV
without accounting for corrections, you get double counting.

## SQL interface

The entity names `check_raw`, `check_current`, `metrics_paid_amount` and
`metrics_paid_hourly` must stay as they are. Internal tables can be named as
you like. The default database is `lab02`; its name can be configured. A
consumer of this interface needs SELECT on all four tables/views.

`check_raw` and `check_current` return the following columns:

```text
dataset_id   String
event_id     String
order_id     String
campaign_id  String
status       String
version      UInt32
amount_cents Int64
is_deleted   UInt8 (0 or 1)
created_at   DateTime64(3, 'UTC')
event_time   DateTime64(3, 'UTC')
```

Consumers run a plain SELECT without FINAL: the choice of the latest version
must happen inside `check_current`. Extra columns are allowed.

Both metric views take the parameter `p_dataset_id:String`.
Columns of `metrics_paid_amount`:

```text
dataset_id        String
paid_amount_cents Int64
```

Columns of `metrics_paid_hourly`:

```text
dataset_id                   String
campaign_id                  String
bucket_start                 DateTime64(3, 'UTC')
paid_amount_cents            Int64
cumulative_paid_amount_cents Int64
```

Each hourly row is unique by `(dataset_id, campaign_id, bucket_start)`. Use
ORDER BY to sort the result.

## Expected behavior

| Scenario                                          | Expected outcome                                                    |
|---------------------------------------------------|---------------------------------------------------------------------|
| Duplicate event and duplicate of the whole batch  | Raw grows; current and both metrics stay unchanged                  |
| v1 -> v7 -> v3 with the same event_time           | v7 wins                                                             |
| Paid order amount changes up and down             | Total, hourly and running sums change accordingly                   |
| Only campaign_id changes                          | Total sum is preserved; the order moves to another campaign         |
| paid -> cancelled                                 | The order disappears from both metrics                              |
| Deletion, then an old live version                | The order stays deleted and does not return to the metrics          |
| Correction of an order from a past hour           | The amount and the following running totals change                  |
| Hour or day change, empty hours                   | The hour is defined by created_at in UTC; accumulation is not reset |
| Empty dataset, all deleted, or only unpaid        | Total sum is 0, no hourly rows                                      |
| Paid with amount 0                                | An hourly row exists                                                |
| Amounts of 1 000 000 000 and totals above UInt32  | No overflow or loss of precision                                    |
| Same order_id in different dataset_id values      | History, current and metrics of the datasets are independent        |

The solution must also hold up under the large load described in
[Before you start](#before-you-start): 200 thousand orders, one million
publications, duplicates, corrections and old versions, with history, current
and both metrics staying correct throughout. There is no corrupted JSON and
no conflicting state for the same version in the base scenario.
