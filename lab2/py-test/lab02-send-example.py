#!/usr/bin/env python3

import json
from pathlib import Path

from confluent_kafka import Producer


def main(topic: str, broker: str, path: Path):
    producer = Producer({
        "bootstrap.servers": broker,
        "enable.idempotence": True,
        "delivery.timeout.ms": 10000,
    })
    errors = []
    count = 0

    def _on_delivery(error, _):
        if error:
            errors.append(error)

    with path.open(encoding="utf-8") as source:
        for line in source:
            event = json.loads(line)
            key = json.dumps(
                {
                    "dataset_id": event["dataset_id"],
                    "order_id": event["order_id"],
                },
                separators=(",", ":"),
            ).encode("utf-8")
            producer.produce(
                topic,
                key=key,
                value=line.rstrip("\n").encode("utf-8"),
                on_delivery=_on_delivery,
            )
            count += 1

    pending = producer.flush(15)
    if errors or pending:
        reason = errors[0] if errors else f'осталось {pending} сообщений'
        raise SystemExit(f"Не удалось отправить все события: {reason}")
    print(f"Отправлено {count} событий в {topic}.")


if __name__ == "__main__":
    main(
        topic="lab02.orders",
        broker="127.0.0.1:19092",
        path=Path(__file__).with_name("lab02-orders-example.jsonl"),
    )
