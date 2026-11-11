"""Ghost Market controls. Python standard library; run through the root justfile."""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "ghost-market/.current-order"
URL = "http://localhost:28083/connectors/ghost-market-cdc"


def sql(statement):
    print(statement, flush=True)
    subprocess.run(
        [  # ruff: ignore[start-process-with-partial-path]
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "cowboy",
            "-d"
            "ghost_market",
            "-v",
            "ON_ERROR_STOP=1",
        ],
        input=statement,
        text=True,
        cwd=ROOT,
        check=True,
    )


def rest(suffix, method="GET", data=None):
    request = urllib.request.Request(
        URL + suffix,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = response.read()
        return json.loads(body) if body else None


def wait_state(expected):
    deadline = time.monotonic() + 60
    status = None
    while time.monotonic() < deadline:
        try:
            status = rest("/status")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            time.sleep(0.5)
            continue
        tasks = status.get("tasks", [])
        if status["connector"]["state"] == expected and (
            expected == "STOPPED"
            or (tasks and all(task["state"] == expected for task in tasks))
        ):
            print(json.dumps(status, indent=2))
            return
        if any(task["state"] == "FAILED" for task in tasks):
            raise RuntimeError(json.dumps(status, indent=2))
        time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for {expected}: {status}")


def current():
    if not STATE.exists() or not STATE.read_text().strip():
        raise RuntimeError("Run just insert first")
    identifier = STATE.read_text().strip()
    if not identifier.startswith("ghost_") or not identifier[6:].isalnum():
        raise RuntimeError("Invalid ghost-market/.current-order")
    return identifier


def main(action):
    match action:
        case "register":
            rest("/config", "PUT", (ROOT / "ghost-market/connector.json").read_bytes())
            wait_state("RUNNING")
        case "status":
            print(json.dumps(rest("/status"), indent=2))
        case "stop" | "resume":
            rest("/" + action, "PUT")
            wait_state("STOPPED" if action == "stop" else "RUNNING")
        case "insert" | "rollback":
            identifier = "ghost_" + uuid.uuid4().hex[:12]
            statement = (
                f"INSERT INTO orders (id,status,amount) "
                f"VALUES ('{identifier}','created',149.90);"
            )
            sql(
                "BEGIN;\n" + statement + "\nROLLBACK;"
                if action == "rollback"
                else statement
            )
            if action == "insert":
                STATE.write_text(identifier)
            print(f"Event key to look for: {identifier}")
        case "pay" | "update" | "delete" | "schema":
            identifier = current()
            statements = {
                "pay": f"UPDATE orders SET status='paid' WHERE id='{identifier}';",
                "update": f"UPDATE orders SET amount=199.90 WHERE id='{identifier}';",
                "delete": f"DELETE FROM orders WHERE id='{identifier}';",
                "schema": (
                    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS comment TEXT;\n"
                    f"UPDATE orders SET comment='erase_after_dawn' WHERE id='{identifier}';"
                ),
            }
            sql(statements[action])
        case "identity-default" | "identity-full":
            sql(
                "ALTER TABLE orders REPLICA IDENTITY "
                + ("DEFAULT;" if action == "identity-default" else "FULL;")
            )
        case "rows":
            sql("TABLE orders;")
        case "slots":
            sql(
                "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, "
                "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
                "AS retained_wal FROM pg_replication_slots;"
            )
        case "pg-config":
            sql(
                "SHOW wal_level;\nSELECT * FROM pg_publication_tables;\n"
                "SELECT relname, relreplident FROM pg_class WHERE relname='orders';"
            )
        case _:
            raise RuntimeError(f"Unknown action: {action}")


if __name__ == "__main__":
    try:
        main(sys.argv[1])
    except (RuntimeError, OSError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
