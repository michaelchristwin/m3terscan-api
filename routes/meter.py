"""
APIRouter module for meter endpoint.
"""

import asyncio
import calendar
import datetime
import json
from collections import defaultdict
from datetime import timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Query
from glide import ExpirySet, ExpiryType
from gql import gql
from sqlmodel import Session, select

from config import graphql, valkey_client
from database import engine
from handlers import daily
from models.output import (
    ActivitiesResponse,
    DailyResponse,
    MonthOfYearResponse,
    WeeklyResponse,
)
from models.weeks_of_year import WeeksEnergy
from schemas.weeks_of_year import WeeksEnergyRead
from utils import latest_block

meter_router = APIRouter(prefix="/meter/{meter_id}")


@meter_router.get("/daily", response_model=List[DailyResponse])
async def get_daily(meter_id: int):
    """
    Get daily energy usage aggregate.
    """
    try:
        return await daily.get_daily_with_cache(meter_id)
    except RuntimeError as e:
        print(f"Cache error: {e}. Falling back to non-cached version.")
        return await daily.get_daily_without_cache(meter_id)


@meter_router.get("/daily-batch")
async def get_daily_batch(
    meter_ids: List[int] = Query(
        ..., description="Repeat param: ?meter_ids=1&meter_ids=2"
    ),
) -> Dict[str, Any]:
    """
    Get daily Batch
    """

    async def run_one(meter_id: int):
        data = await daily.get_daily_with_cache(meter_id)
        return meter_id, data

    results = await asyncio.gather(*(run_one(mid) for mid in meter_ids))

    return {str(meter_id): data for meter_id, data in results}


@meter_router.get("/weekly", response_model=List[WeeklyResponse])
async def get_weekly(meter_id: int):
    """
    Get weekly energy usage aggregate.
    """
    height, _ = latest_block.get_latest_block()
    interval = datetime.datetime.now().weekday() * 720
    min_block = height - interval

    async def fetch_page(cursor: str | None = None):
        variables = {
            "meterNumber": meter_id,
            "block": {"min": min_block, "max": height},
            "first": 1000,
            "sortBy": "HEIGHT_ASC",
            "after": None,
        }
        if cursor:
            variables["after"] = cursor
        query = gql(
            """
        query WeeklyQuery($meterNumber: Int!,
                         $block: BlockFilter,
                         $first: Int!,
                         $sortBy: MeterDataPointOrderBy,
                         $after: String) {
        meterDataPoints(meterNumber: $meterNumber,
                         block: $block,
                         first: $first,
                         sortBy: $sortBy,
                         after: $after) {
                node {
                    timestamp
                    payload {
                        energy
                    }
                }
                cursor
            }
        }
        """
        )
        query.variable_values = variables
        result = await graphql.gql_query(query)
        items = result.get("meterDataPoints", [])

        return [
            {
                "timestamp": i["node"]["timestamp"],
                "energy": i["node"]["payload"]["energy"],
                "cursor": i["cursor"],
            }
            for i in items
        ]

    all_flat_data = []
    cursor = None

    while True:
        page = await fetch_page(cursor)

        if not page:
            break
        all_flat_data.extend(page)
        cursor = page[-1]["cursor"]

    # Handle the empty array case:
    if not all_flat_data:
        return [{"day_of_week": i, "total_energy": 0.0} for i in range(7)]

    now_utc = datetime.datetime.now(tz=timezone.utc)

    # 2. Calculate the start of the current week (Monday at 00:00:00 UTC)
    # The .weekday() method returns 0 for Monday and 6 for Sunday.
    days_since_monday = now_utc.weekday()
    start_of_week_utc = (now_utc - datetime.timedelta(days=days_since_monday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # 3. Calculate the end of the current week (The first millisecond of next Monday)
    end_of_week_utc = start_of_week_utc + datetime.timedelta(weeks=1)

    # 4. Convert boundary times to milliseconds for comparison
    start_ms = int(start_of_week_utc.timestamp() * 1000)
    end_ms = int(end_of_week_utc.timestamp() * 1000)

    # 5. Sanitize: keep only data within the current UTC week [start_ms, end_ms)
    all_flat_data = [
        item for item in all_flat_data if start_ms <= item["timestamp"] < end_ms
    ]
    if not all_flat_data:
        return [{"day_of_week": i, "total_energy": 0.0} for i in range(7)]
    daily_energy = defaultdict(float)

    for row in all_flat_data:
        # timestamp is in ms
        dt = datetime.datetime.fromtimestamp(row["timestamp"] / 1000, tz=timezone.utc)
        day_of_week = dt.weekday()  # 0 = Monday, 6 = Sunday
        daily_energy[day_of_week] += row["energy"]

    # ensure all 7 days exist
    daily_filled = {day: daily_energy.get(day, 0.0) for day in range(7)}

    output_array = [
        {
            # Note: 0=Mon, 6=Sun
            "day_of_week": day,
            "total_energy": round(energy, 6),
        }
        for day, energy in daily_filled.items()
    ]

    return output_array


@meter_router.get("/weeks/{year}", response_model=List[WeeksEnergyRead])
async def get_weeks_of_year(meter_id: int, year: int):
    """
    Get energy usage of all weeks of specified year.
    """
    genesis_year = 2025
    blocks_per_day = 720  # 1 block every 2 minutes

    if year < genesis_year:
        raise ValueError(f"No data exists before {genesis_year}")

    now = datetime.datetime.now(timezone.utc)
    today = now.date()
    current_year = now.year

    if year > current_year:
        raise ValueError("Cannot calculate future year blocks")

    async def fetch_page(min_block, max_block, cursor=None):
        variables = {
            "meterNumber": meter_id,
            "block": {"min": min_block, "max": max_block},
            "first": 3000,
            "sortBy": "HEIGHT_ASC",
            "after": cursor,
        }

        query = gql(
            """
            query WeeksQuery(
              $meterNumber: Int!,
              $block: BlockFilter,
              $first: Int!,
              $sortBy: MeterDataPointOrderBy,
              $after: String
            ) {
              meterDataPoints(
                meterNumber: $meterNumber,
                block: $block,
                first: $first,
                sortBy: $sortBy,
                after: $after
              ) {
                node {
                  timestamp
                  payload { energy }
                }
                cursor
              }
            }
            """
        )

        query.variable_values = variables
        result = await graphql.gql_query(query)

        return [
            {
                "timestamp": i["node"]["timestamp"],
                "energy": i["node"]["payload"]["energy"],
                "cursor": i["cursor"],
            }
            for i in result.get("meterDataPoints", [])
        ]

    def total_weeks_in_year(y: int) -> int:
        dec28 = datetime.date(y, 12, 28)
        return dec28.isocalendar()[1]

    def build_zero_output(y: int):
        return [
            {"week": w, "total_energy": 0.0, "year": y}
            for w in range(1, total_weeks_in_year(y) + 1)
        ]

    def aggregate_weekly(data):
        weekly = defaultdict(float)

        for row in data:
            dt = datetime.datetime.fromtimestamp(
                row["timestamp"] / 1000, tz=timezone.utc
            )
            iso_year, iso_week, _ = dt.isocalendar()
            if iso_year == year:
                weekly[iso_week] += row["energy"]

        return weekly

    def to_output(weekly):
        return [
            {
                "week": w,
                "total_energy": round(float(weekly.get(w, 0.0)), 6),
                "year": year,
            }
            for w in range(1, total_weeks_in_year(year) + 1)
        ]

    height, _ = latest_block.get_latest_block()

    with Session(engine) as session:
        statement = select(WeeksEnergy).where(WeeksEnergy.year == year)
        db_results = session.exec(statement).all()

        # --------------------------------------------------
        # INITIAL POPULATION (no DB records exist)
        # --------------------------------------------------
        if not db_results:
            start_of_year = datetime.date(year, 1, 1)
            end_of_year = today if year == current_year else datetime.date(year, 12, 31)

            days_in_range = (end_of_year - start_of_year).days + 1
            days_since_start = (today - start_of_year).days

            if year == current_year:
                min_block = height - (days_since_start * blocks_per_day)
                max_block = height
            else:
                days_after_year = (today - end_of_year).days
                max_block = height - (days_after_year * blocks_per_day)
                min_block = max_block - (days_in_range * blocks_per_day)

            all_data = []
            cursor = None

            while True:
                page = await fetch_page(min_block, max_block, cursor)
                if not page:
                    break
                all_data.extend(page)
                cursor = page[-1]["cursor"]

            if not all_data:
                data_output = build_zero_output(year)
            else:
                start_ms = int(
                    datetime.datetime(year, 1, 1, tzinfo=timezone.utc).timestamp()
                    * 1000
                )
                end_ms = int(
                    datetime.datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp()
                    * 1000
                )

                filtered = [d for d in all_data if start_ms <= d["timestamp"] < end_ms]

                weekly = aggregate_weekly(filtered)
                data_output = to_output(weekly)

            records = [WeeksEnergy(**item) for item in data_output]
            session.add_all(records)
            session.commit()

            return data_output

        # --------------------------------------------------
        # CURRENT YEAR UPDATE (only update current week)
        # --------------------------------------------------
        if year == current_year:
            week_number = today.isocalendar().week

            week_start = today - datetime.timedelta(days=today.weekday())
            week_start_dt = datetime.datetime.combine(
                week_start, datetime.time.min, tzinfo=timezone.utc
            )

            seconds_since_week_start = (now - week_start_dt).total_seconds()
            blocks_since_week_start = int(seconds_since_week_start / 120)

            min_block = height - blocks_since_week_start

            all_data = []
            cursor = None

            while True:
                page = await fetch_page(min_block, height, cursor)
                if not page:
                    break
                all_data.extend(page)
                cursor = page[-1]["cursor"]

            if all_data:
                weekly = aggregate_weekly(all_data)
                current_week_energy = round(float(weekly.get(week_number, 0.0)), 6)

                stmt = select(WeeksEnergy).where(
                    (WeeksEnergy.year == year) & (WeeksEnergy.week == week_number)
                )

                existing = session.exec(stmt).one_or_none()

                if existing:
                    existing.total_energy = current_week_energy
                else:
                    session.add(
                        WeeksEnergy(
                            year=year,
                            week=week_number,
                            total_energy=current_week_energy,
                        )
                    )

                session.commit()

            # always return fresh DB state
            db_results = session.exec(statement).all()

        return [
            {
                "week": r.week,
                "total_energy": r.total_energy,
                "year": r.year,
            }
            for r in db_results
        ]


@meter_router.get("/month/{year}/{month}", response_model=List[MonthOfYearResponse])
async def get_month_of_year(
    meter_id: int,
    year: int,
    month: int,
):
    """
    Get energy usage of a month in a specified year using pagination and timestamp filtering.
    """
    genesis_year = 2025
    if year < genesis_year:
        raise ValueError(f"No data exists before {genesis_year}")

    vc = valkey_client.ValkeyManager.get_client()
    cache_key = f"energy:{meter_id}:month:{year}:{month}"
    cached = await vc.get(cache_key)
    if cached:
        return json.loads(cached)
    height, _ = latest_block.get_latest_block()
    today = datetime.date.today()
    target = datetime.date(year, month, 1)
    if target > today.replace(day=1):
        raise ValueError("Future month not allowed")
    current_year = datetime.datetime.now(timezone.utc).year

    # --- BLOCK RANGE ESTIMATION (For initial GraphQL fetch) ---
    # This range is just a safeguard; the timestamp filter is the precise boundary.
    blocks_per_day = 720
    last_day = calendar.monthrange(year, month)[1]

    # Estimate the difference in days from today to the start of the target month
    days_diff_to_start = (today - target).days
    min_block = height - (days_diff_to_start * blocks_per_day)
    min_block = max(1, min_block)

    # Estimate the difference in blocks for the duration of the month
    blocks_in_month = last_day * blocks_per_day

    # Max block is the minimum of the estimated end block or the current chain height
    max_block = min(min_block + blocks_in_month, height)

    # --- PAGINATION LOGIC ---

    async def fetch_page(cursor: str | None = None):
        """Fetches a page of data from the GraphQL endpoint."""
        variables = {
            "meterNumber": meter_id,
            "block": {"min": min_block, "max": max_block},
            "first": 2000,  # Set as requested
            "sortBy": "HEIGHT_ASC",
            "after": cursor,
        }

        # Note: Added $first, $sortBy, and $after variables to the query definition
        query = gql(
            """
        query MonthQuery($meterNumber: Int!,
                         $block: BlockFilter,
                         $first: Int!,
                         $sortBy: MeterDataPointOrderBy,
                         $after: String) {
        meterDataPoints(meterNumber: $meterNumber,
                         block: $block,
                         first: $first,
                         sortBy: $sortBy,
                         after: $after) {
                node {
                    timestamp
                    payload {
                        energy
                    }
                }
                cursor
            }
        }
        """
        )
        query.variable_values = variables
        result = await graphql.gql_query(query)
        items = result.get("meterDataPoints", [])

        # Flatten data and include the cursor for the next iteration
        return [
            {
                "timestamp": i["node"]["timestamp"],
                "energy": i["node"]["payload"]["energy"],
                "cursor": i["cursor"],
            }
            for i in items
        ]

    all_flat_data = []
    cursor = None

    while True:
        page = await fetch_page(cursor)

        if not page:
            break
        all_flat_data.extend(page)
        cursor = page[-1][
            "cursor"
        ]  # Use the cursor of the last item for the next fetch

    # --- FILTERING BY CALENDAR MONTH (UTC) ---

    # 1. Define the start of the target month (1st of month, 00:00:00.000 UTC)
    start_of_month_utc = datetime.datetime(year, month, 1, tzinfo=timezone.utc)

    # 2. Define the start of the next month (1st of next month, 00:00:00.000 UTC)
    if month == 12:
        end_of_month_utc = datetime.datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_of_month_utc = datetime.datetime(year, month + 1, 1, tzinfo=timezone.utc)

    # 3. Convert boundary times to milliseconds for comparison
    start_ms = int(start_of_month_utc.timestamp() * 1000)
    end_ms = int(end_of_month_utc.timestamp() * 1000)

    # 4. Sanitize: keep only data within the target calendar month [start_ms, end_ms)
    all_flat_data = [
        item for item in all_flat_data if start_ms <= item["timestamp"] < end_ms
    ]

    # ------------------------------------------------------------------
    # 3. AGGREGATION AND ZERO-FILLING
    # ------------------------------------------------------------------

    # Re-check for empty data after filtering:
    if not all_flat_data:
        # If empty, return an array of 0 energy for every day in the month
        output_array = [{"day": i, "total_energy": 0.0} for i in range(1, last_day + 1)]
        return output_array

    daily_energy = defaultdict(float)

    for row in all_flat_data:
        dt = datetime.datetime.fromtimestamp(row["timestamp"] / 1000, tz=timezone.utc)
        day = dt.day  # 1 to 31
        daily_energy[day] += row["energy"]

    # Fill all days of the month
    output_array = [
        {"day": day, "total_energy": round(float(daily_energy.get(day, 0.0)), 6)}
        for day in range(1, last_day + 1)
    ]

    # Create DataFrame and convert ms timestamp to UTC datetime
    if year < current_year:
        await vc.set(cache_key, json.dumps(output_array))
    else:
        # Current year → still useful, but allow expiry
        await vc.set(
            cache_key,
            json.dumps(output_array),
            expiry=ExpirySet(value=(6 * 3600), expiry_type=ExpiryType.SEC),
        )
    return output_array


@meter_router.get("/activities", response_model=ActivitiesResponse)
async def get_activities(meter_id: int, after: str | None = None, limit: int = 10):
    """
    Get activities of a m3ter

     :param m3ter_id: Description
     :type m3ter_id: int
    """
    if limit < 1:
        limit = 10

    query = gql(
        """
        query ActivitiesQuery($meterNumber: Int!, $first: Int!, $after: String) {
		    meterDataPoints(
				meterNumber: $meterNumber,
				first: $first,
				after: $after,
				sortBy: HEIGHT_DESC
			    ) {
				cursor
				node {
					timestamp
					payload {
						energy
						signature
					}
				}
			  }
		    }"""
    )
    query.variable_values = {"meterNumber": meter_id, "first": limit, " after": after}

    result = await graphql.gql_query(query)
    meter_data_points = result.get("meterDataPoints", [])
    activities = []
    next_cursor = ""

    for i, item in enumerate(meter_data_points):
        node = item["node"]

        activities.append(
            {
                "timestamp": int(node["timestamp"]),
                "energy": float(node["payload"]["energy"]),
                "signature": node["payload"].get("signature", ""),
            }
        )

        # last cursor
        if i == len(meter_data_points) - 1:
            next_cursor = item.get("cursor", "")

    return {
        "data": activities,
        "limit": limit,
        "next_cursor": next_cursor,
    }
