import asyncio
import os
import clickhouse_connect
import httpx

from contextlib import asynccontextmanager
from pathlib import Path as FilePath
from typing import Annotated, Literal
from datetime import UTC, datetime, timedelta
from clickhouse_connect.driver.exceptions import ClickHouseError
from fastapi import FastAPI, HTTPException, Query, Request, Path
from pydantic import BaseModel, ConfigDict, Field, AwareDatetime, model_validator

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "lab02")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "checker")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "checker")

SUMMARY_SQL= (FilePath(__file__).parent / "queries/summary.sql").read_text()
METADATA_SQL = (FilePath(__file__).parent / "queries/metadata.sql").read_text()
ORDER_SQL = (FilePath(__file__).parent / "queries/order.sql").read_text()

ID_PATTERN=r"^[a-z0-9][a-z0-9_-]{0,63}$"

class ForbiddenParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

class SummaryAmount(BaseModel):
    paid_amount_cents: int

class MetaData(BaseModel):
    computed_at: str
    latest_event_at: str | None

class SummaryMetric(BaseModel):
    data: SummaryAmount
    meta: MetaData

class TimeSeriesData(BaseModel):
    campaign_id: str
    bucket_start: str
    paid_amount_cents: int
    cumulative_paid_amount_cents: int

class TimeSeriesMetric(BaseModel):
    data: list[TimeSeriesData]
    meta: MetaData

class OrderData(BaseModel):
    order_id: str
    campaign_id: str
    version: int
    status: Literal["created", "paid", "cancelled"]
    amount_cents: int
    created_at: str
    event_time: str

class OrdersPage(BaseModel):
    data: list[OrderData]
    next_after_order_id: str | None

class OrderResponse(BaseModel):
    data: OrderData

class OrderFilters(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["created", "paid", "cancelled"] | None = None
    campaign_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None
    limit: Annotated[int, Field(ge=1, le=100)] = 50
    after_order_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None

class SeriesFilters(BaseModel):
    model_config = ConfigDict(extra="forbid")
    from_: Annotated[AwareDatetime, Field(alias="from")]
    to: AwareDatetime
    campaign_id: Annotated[str | None, Field(pattern=ID_PATTERN)] = None

    @model_validator(mode="after")
    def check_range(self):
        self.from_ = self.from_.astimezone(UTC)
        self.to = self.to.astimezone(UTC)
        if self.from_.minute or self.from_.second or self.from_.microsecond:
            raise ValueError("from and to must be exactly on the hour (UTC)")
        if self.to.minute or self.to.second or self.to.microsecond:
            raise ValueError("from and to must be exactly on the hour (UTC)")
        if self.from_ >= self.to:
            raise ValueError("from must be earlier than to")
        if self.to - self.from_ > timedelta(days=31):
            raise ValueError("range must be below 31 days")
        return self

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.gate = asyncio.Semaphore(4)
    async with httpx.AsyncClient(
        base_url=CLICKHOUSE_URL,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=5,
        trust_env=False,
    ) as http_ch_client:
        app.state.http_ch_client = http_ch_client
        async with await clickhouse_connect.get_async_client(
            dsn=CLICKHOUSE_URL,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
            connect_timeout=5,
            send_receive_timeout=5,
        ) as ch_client:
            app.state.ch_client = ch_client
            yield

app = FastAPI(lifespan=lifespan)

@app.get("/ping")
async def ping():
    return {"status": "ok"}

@app.get("/example/orders-client")
async def example_orders_client(
    request: Request,
    dataset_id: Annotated[str, Path(pattern=ID_PATTERN)],
    limit: Annotated[int, Query(ge=1, le=100)] = 5,
):
    try:
        await asyncio.wait_for(request.app.state.gate.acquire(), timeout=0.5)
    except TimeoutError:
        raise HTTPException(429, "API busy") from None
    try:
        async with asyncio.timeout(5):
            result = await request.app.state.ch_client.query(
                """
                    SELECT order_id, status, amount_cents FROM check_current
                    WHERE dataset_id={dataset_id:String} AND is_deleted=0
                    ORDER BY order_id LIMIT {limit:UInt32}
                """,
                parameters={"dataset_id": dataset_id, "limit": limit},
                settings={"max_execution_time": 5},
            )
            return {"data": list(result.named_results())}
    except (ClickHouseError, TimeoutError, OSError):
        raise HTTPException(503, "ClickHouse unavailable") from None
    finally:
        request.app.state.gate.release()

async def db_get_order(request: Request, dataset_id: str, order_id: str) -> OrderData | None:
    result = await request.app.state.ch_client.query(
            ORDER_SQL, parameters={"dataset_id": dataset_id, "order_id": order_id}, settings={"max_execution_time": 5}
    )
    if not result.row_count:
        return None
    return OrderData(**dict(zip(OrderData.model_fields, result.first_row)))

async def db_get_orders(request: Request, dataset_id: str, f: OrderFilters) -> OrdersPage:
    params = {"dataset_id": dataset_id, "limit": f.limit + 1}

    sql =  """
        SELECT order_id, campaign_id, version, status, amount_cents,
               replace(toString(toDateTime64(created_at, 3, 'UTC')), ' ', 'T') || 'Z',
               replace(toString(toDateTime64(event_time, 3, 'UTC')), ' ', 'T') || 'Z'
          FROM check_current
         WHERE dataset_id = {dataset_id:String}
               AND is_deleted = 0
    """
    if not f.status is None:
         sql += " AND status = {status:String}\n"
         params["status"] = f.status

    if not f.campaign_id is None:
         sql += " AND campaign_id = {campaign_id:String}\n"
         params["campaign_id"] = f.campaign_id

    if not f.after_order_id is None:
         sql += " AND order_id > {after_order_id:String}\n"
         params["after_order_id"] = f.after_order_id
    sql += " ORDER BY order_id LIMIT {limit:UInt32}"

    result = await request.app.state.ch_client.query(
        sql, parameters=params, settings={"max_execution_time": 5}
    )
    rows = [OrderData(**dict(zip(OrderData.model_fields, row))) for row in result.result_rows]
    page = rows[: f.limit]
    next_id = page[-1].order_id if len(rows) > f.limit else None
    return OrdersPage(data=page, next_after_order_id=next_id)

@app.get("/v1/datasets/{dataset_id}/orders", response_model=OrdersPage) 
async def orders(
    request: Request,    
    dataset_id: Annotated[str, Path(pattern=ID_PATTERN)],
    filters: Annotated[OrderFilters, Query()],
):
    try:
        await asyncio.wait_for(request.app.state.gate.acquire(), timeout=0.5)
    except TimeoutError:
        raise HTTPException(429, "API busy") from None
    try:
        async with asyncio.timeout(5):
            page = await db_get_orders(request, dataset_id, filters)
    except (ClickHouseError, TimeoutError, OSError):
        raise HTTPException(503, "ClickHouse unavailable") from None
    finally:
        request.app.state.gate.release()
    return page

@app.get("/v1/datasets/{dataset_id}/orders/{order_id}", response_model=OrderResponse) 
async def get_order(
    request: Request,
    dataset_id: Annotated[str, Path(pattern=ID_PATTERN)],
    order_id: Annotated[str, Path(pattern=ID_PATTERN)],
    q: Annotated[ForbiddenParameters, Query()]
):
    try:
        await asyncio.wait_for(request.app.state.gate.acquire(), timeout=0.5)
    except TimeoutError:
        raise HTTPException(429, "API busy") from None
 
    try:
        async with asyncio.timeout(5):
            result = await db_get_order(request, dataset_id, order_id)
    except (ClickHouseError, TimeoutError, OSError):
        raise HTTPException(503, "ClickHouse unavailable") from None
    finally:
        request.app.state.gate.release()
    
    if result is None:
        raise HTTPException(404, "Order not found")
    return OrderResponse(data=result)

async def get_summary(request: Request, dataset_id: str) -> SummaryAmount | None:
    result = await request.app.state.ch_client.query(
            SUMMARY_SQL, parameters={"dataset_id": dataset_id}, settings={"max_execution_time": 5}
     )
    if not result.row_count:
        return None
    return SummaryAmount(paid_amount_cents=result.first_row[0])

async def get_metadata(request: Request, dataset_id: str) -> MetaData | None:
    result = await request.app.state.ch_client.query(
            METADATA_SQL, parameters={"dataset_id": dataset_id}, settings={"max_execution_time": 5}
    )
    if not result.row_count:
        return None
    computed_at, latest_event_at = result.first_row
    return MetaData(computed_at=computed_at, latest_event_at=latest_event_at)

@app.get("/v1/datasets/{dataset_id}/metrics/summary", response_model=SummaryMetric)
async def order_summary(
    request: Request,
    dataset_id: Annotated[str, Path(pattern=ID_PATTERN)],
    q: Annotated[ForbiddenParameters, Query()]
):
    try:
        await asyncio.wait_for(request.app.state.gate.acquire(), timeout=0.5)
    except TimeoutError:
        raise HTTPException(429, "API busy") from None
    try:
        async with asyncio.timeout(5):
            data = await get_summary(request, dataset_id)
            meta = await get_metadata(request, dataset_id)
    except (ClickHouseError, TimeoutError, OSError):
        raise HTTPException(503, "Clickhouse Unavailable") from None
    finally:
        request.app.state.gate.release()
    return SummaryMetric(data=data,meta=meta)

def fill_series(rows, from_ts, n_hours, campaign_filter) -> list[TimeSeriesData]:
    history = {}   # campaign -> [(ts, paid, cumulative)], already sorted by ts
    for campaign, ts, paid, cum in rows:
        history.setdefault(campaign, []).append((ts, paid, cum))

    campaigns = [campaign_filter] if campaign_filter else sorted(history)
    out = []
    for campaign in campaigns:
        hours = history.get(campaign, [])
        by_ts = {ts: (paid, cum) for ts, paid, cum in hours}

        cumulative = 0
        for ts, _, cum in hours:   
            if ts < from_ts:
                cumulative = cum

        for i in range(n_hours):
            ts = from_ts + 3600 * i
            paid, cumulative = by_ts.get(ts, (0, cumulative))   # empty hour: paid 0, total unchanged
            out.append(TimeSeriesData(
                campaign_id=campaign,
                bucket_start=datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                paid_amount_cents=paid,
                cumulative_paid_amount_cents=cumulative,
            ))
    return out

async def db_get_timeseries(request: Request, dataset_id: str, f: SeriesFilters) -> list[TimeSeriesData]:
    sql = """
        SELECT campaign_id,
               toUnixTimestamp(bucket_start),
               paid_amount_cents,
               cumulative_paid_amount_cents
          FROM metrics_paid_hourly(p_dataset_id = {dataset_id:String})
    """
    params = {"dataset_id": dataset_id}
    if f.campaign_id is not None:
        sql += " WHERE campaign_id = {campaign_id:String}\n"
        params["campaign_id"] = f.campaign_id
    sql += " ORDER BY campaign_id, bucket_start"

    result = await request.app.state.ch_client.query(
        sql, parameters=params, settings={"max_execution_time": 5}
    )
    from_ts = int(f.from_.timestamp())
    n_hours = (int(f.to.timestamp()) - from_ts) // 3600
    return fill_series(result.result_rows, from_ts, n_hours, f.campaign_id)

@app.get("/v1/datasets/{dataset_id}/metrics/timeseries", response_model=TimeSeriesMetric)
async def metrics_timeseries(
    request: Request,
    dataset_id: Annotated[str, Path(pattern=ID_PATTERN)],
    f: Annotated[SeriesFilters, Query()],
):
    try:
        await asyncio.wait_for(request.app.state.gate.acquire(), timeout=0.5)
    except TimeoutError:
        raise HTTPException(429, "API busy") from None
    try:
        async with asyncio.timeout(5):
            data = await db_get_timeseries(request, dataset_id, f)
            meta = await get_metadata(request, dataset_id)
    except (ClickHouseError, TimeoutError, OSError):
        raise HTTPException(503, "ClickHouse unavailable") from None
    finally:
        request.app.state.gate.release()
    return TimeSeriesMetric(data=data, meta=meta)
