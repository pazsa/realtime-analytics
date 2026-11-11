# Lab 01. CDC from PostgreSQL to Kafka

Set up a `PostgreSQL -> Debezium -> Kafka` pipeline for an orders database.
Changes must reach Kafka on record creation, update and deletion, and after
the connector is stopped, processing must resume without losing new data.

## Task

Build a standalone lab environment with PostgreSQL, Kafka and Kafka Connect
with Debezium. Take the table schema and control data from the
[data contract](#data-contract). Prepare the DDL, data loading, Compose file
and connector configuration yourself. Choose the startup order and configure
permissions, logical replication, publication, replication slot and network
reachability between components.

Tested versions: PostgreSQL 16, Kafka 4.2.1, Debezium Connect 3.3.1.Final.
If you pick other versions, make sure the Connect REST API supports
`stop`/`resume`, which are used to test recovery.

## Requirements

- PostgreSQL and Kafka Connect are reachable; the connector and its tasks are
  in the `RUNNING` state.
- The publication contains the three business tables, an active logical slot
  uses `pgoutput`, and the replication user has the required privileges.
- Initial records from `orders`, `order_items` and `payments` land in Kafka as
  snapshot events. The internal `internal_notes` table stays out of CDC.
- A new order produces `op=c` with the correct key, fields and source.
- An order update produces `op=u`; a delete produces `op=d` followed by a
  tombstone. Update and delete events must carry the full old row image.
- An order created while the connector is stopped is delivered as a live event
  after `resume`. Within the checked offset range there is no repeated
  snapshot and exactly one create event for that order. This is a bounded
  recovery check, not a proof of exactly-once delivery under arbitrary
  failures.

**Load the seed data before the first connector start.** The original snapshot
events for these records must exist in Kafka; a regular INSERT after startup
does not replace them. Keep the snapshot events in Kafka.

If the snapshot fails, first work out what state is left in PostgreSQL, the
replication slot, the Kafka Connect offsets and the output topics. Deleting a
single PostgreSQL volume does not reset the rest of the environment. Before
re-initializing, decide which state to keep and which to recreate consistently.

## Data contract

The examples use the database `flatline` and the schema `public`. Types are
simple: all ids are `TEXT`, timestamps are `TIMESTAMPTZ`, and amounts are
stored as `NUMERIC(12,2)` in conventional credits. Note: exact decimal values
with two digits after the decimal point, **not** plain floats.

```text
orders
  id           TEXT           PRIMARY KEY
  campaign_id  TEXT           NOT NULL
  status       TEXT           NOT NULL
  amount       NUMERIC(12,2)  NOT NULL
  created_at   TIMESTAMPTZ    NOT NULL
```

```text
order_items
  id           TEXT           PRIMARY KEY
  order_id     TEXT           NOT NULL, FK -> orders.id
  sku          TEXT           NOT NULL
  quantity     INTEGER        NOT NULL
  unit_price   NUMERIC(12,2)  NOT NULL
```

```text
payments
  id           TEXT           PRIMARY KEY
  order_id     TEXT           NOT NULL, FK -> orders.id
  status       TEXT           NOT NULL
  amount       NUMERIC(12,2)  NOT NULL
  paid_at      TIMESTAMPTZ    NOT NULL
```

```text
internal_notes
  id           TEXT           PRIMARY KEY
  note         TEXT           NOT NULL
```

`internal_notes` is an internal table and must not be included in CDC.

Before the first connector start, load the following records into the tables.
Values are listed in the column order shown above:

```text
orders:         seed_order_01, chiba_retina, paid, 149.90, 2026-01-01T03:17:00Z
order_items:    seed_item_01, seed_order_01, maternal_pride_subscription, 1, 149.90
payments:       seed_payment_01, seed_order_01, captured, 149.90, 2026-01-01T03:18:00Z
internal_notes: seed_note_01, do-not-export
```

Topics: `<topic.prefix>.<schema>.orders`, `.order_items`, `.payments`. Table
names are fixed. The seed snapshot events must be kept in Kafka: use
`cleanup.policy=delete` with retention of at least seven days, not
compact-only.

### Connector contract

- `connector.class=io.debezium.connector.postgresql.PostgresConnector`,
- `plugin.name=pgoutput`,
- `snapshot.mode=initial`,
- original Debezium envelope,
- JSON key `{ "id": "..." }`; `key.converter` and `value.converter` are both
  `org.apache.kafka.connect.json.JsonConverter`;
  `key.converter.schemas.enable=false` and
  `value.converter.schemas.enable=false`. Set these four parameters explicitly
  in the connector configuration; do not rely on inheritance from the Connect
  worker,
- `table.include.list` explicitly selects only the three business tables of
  the chosen schema, without `internal_notes` or any other existing tables,
- decimals as strings (`decimal.handling.mode=string`),
- time in UTC,
- `slot.drop.on.stop` must not be `true`: the read position is preserved when
  the connector stops,
- `REPLICA IDENTITY FULL` is required on all three tables; tombstones are
  enabled. The publication contains exactly these tables in advance. The
  publication name and slot name are not fixed.

### Permissions

- Replication user: LOGIN, REPLICATION and SELECT.
- Verification user: SELECT on the three business tables, INSERT/UPDATE/DELETE
  on `orders`, USAGE on the schema, and access to replication metadata
  (`pg_read_all_stats`).
- The verification user does not need to be a superuser (and ideally is not).

The PostgreSQL address as seen by the connector may differ from the address
used by other clients (for example, Docker DNS name versus a published host
port). The `database.hostname`, `database.dbname` and `database.user` values
in the connector configuration are set independently.

A controlled stop/resume is verified over a finite offset range: a new row
must produce a single `op=c` and no `op=r` in all three topics. The contract
does not cover concurrent connector management.
