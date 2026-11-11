# realtime-analytics

Labs on Debezium, ClickHouse and FastAPI for real-time analytics. Together
they build one pipeline, end to end:

```text
PostgreSQL -> Debezium -> Kafka -> ClickHouse -> FastAPI -> Prometheus
```

Each lab builds on the previous one. Lab requirements are translated and
adapted from the course materials (NPL Realtime); the solutions are my own.
[Justfiles](https://github.com/casey/just) are used to run commands.

## Labs

- **Lab 01. CDC from PostgreSQL to Kafka**
  - [Lab requirements](lab1/lab01-cdc-postgres-debezium.md)
  - [Solution files](lab1/)
- **Lab 02. ClickHouse and the change event flow**
  - [Lab requirements](lab2/lab02-clickhouse-aggregations.md)
  - [Solution files](lab2/)
- **Lab 03. FastAPI for order analytics**
  - [Lab requirements](lab3/lab03-fastapi-metrics.md)
  - [Solution files](lab3/)
- **Lab 04. Metrics, alerts and cache**
  - [Lab requirements](lab4/lab04-ops-drill.md)
  - [Solution files](lab4/)

## What I've learned

- **CDC with Debezium:** logical replication, tombstones, latency.
- **Handling duplicates:** 
  picking the latest state by `version` instead of arrival time or offset,
  and logical deletes.
- **ClickHouse modeling:** mainly parametrized views were new for me
- **Serving data through an API:** strict pydantic validation, knowing when to use python to not overcomplicate SQL
- **Observability:** Prometheus counters and histograms with low-cardinality
  labels, alert rules for unavailability and error
  rate.
- **Failure behavior:** an in-process TTL cache that serves the last computed
  answer when ClickHouse is down, and how alerts behave when the database
  stops compared to when the API stops.

## What's missing

Prospect things that are missing in current repo:

- Grafana dashboards on top of Prometheus
- Redis as a shared cache, so it survives API restarts
- An incident runbook for the stand
- PostgreSQL + Debezium as a second ingestion path into ClickHouse
