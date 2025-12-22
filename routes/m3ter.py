"""
APIRouter module for m3ter endpoint.
"""

import json
import datetime
import calendar
from typing import List
from datetime import timezone
from collections import defaultdict
from gql import gql
from fastapi import APIRouter
from glide import ExpirySet, ExpiryType
from models import output
from handlers import daily
from utils import latest_block
from config import graphql, valkey_client

m3ter_router = APIRouter(prefix="/m3ter/{m3ter_id}")


@m3ter_router.get("/daily", response_model=List[output.DailyResponse])
async def get_daily(m3ter_id: int):
    """
    Get daily energy usage aggregate.
    """
    try:
        return await daily.get_daily_with_cache(m3ter_id)
    except RuntimeError as e:
        print(f"Cache error: {e}. Falling back to non-cached version.")
        return await daily.get_daily_without_cache(m3ter_id)


@m3ter_router.get("/weekly", response_model=List[output.WeeklyResponse])
async def get_weekly(m3ter_id: int):
    """
    Get weekly energy usage aggregate.
    """
    height, _ = latest_block.get_latest_block()
    interval = datetime.datetime.now().weekday() * 720
    min_block = height - interval

    async def fetch_page(cursor: str | None = None):
        variables = {
            "meterNumber": m3ter_id,
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


@m3ter_router.get("/weeks/{year}", response_model=List[output.WeeksOfYearResponse])
async def get_weeks_of_year(m3ter_id: int, year: int):
    """
    Get energy usage of all weeks of specified year.
    """
    genesis_year = 2025
    if year < genesis_year:
        raise ValueError(f"No data exists before {genesis_year}")
    vc = valkey_client.ValkeyManager.get_client()
    cache_key = f"energy:{m3ter_id}:weeks:{year}"
    current_year = datetime.datetime.now(timezone.utc).year

    # ---- 1. Try cache (safe for past years, acceptable for current year) ----
    cached = await vc.get(cache_key)
    if cached:
        return json.loads(cached)

    # ---- 2. Original computation logic (unchanged) ----
    height, _ = latest_block.get_latest_block()
    today = datetime.date.today()

    blocks_per_week = 5040  # 720 blocks/day * 7

    if year > current_year:
        raise ValueError("Cannot calculate future year blocks")

    if year == current_year:
        weeks_in_year = today.isocalendar()[1]
    else:
        dec28 = datetime.date(year, 12, 28)
        weeks_in_year = dec28.isocalendar()[1]

    total_blocks_in_year = weeks_in_year * blocks_per_week
    blocks_before_year = 0

    for y in range(genesis_year, year):
        dec28 = datetime.date(y, 12, 28)
        weeks = dec28.isocalendar()[1]
        blocks_before_year += weeks * blocks_per_week

    min_block = blocks_before_year + 1
    max_block = blocks_before_year + total_blocks_in_year

    if year == current_year:
        max_block = height

    async def fetch_page(cursor: str | None = None):
        variables = {
            "meterNumber": m3ter_id,
            "block": {"min": min_block, "max": max_block},
            "first": 5000,
            "sortBy": "HEIGHT_ASC",
            "after": cursor,
        }

        query = gql(
            """
            query WeeklyQuery(
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

    all_flat_data = []
    cursor = None

    while True:
        page = await fetch_page(cursor)
        if not page:
            break
        all_flat_data.extend(page)
        cursor = page[-1]["cursor"]

    if not all_flat_data:
        dec28 = datetime.date(year, 12, 28)
        total_weeks = dec28.isocalendar()[1]
        data_output = [
            {"week": i, "total_energy": 0.0} for i in range(1, total_weeks + 1)
        ]

    else:
        start_of_year_utc = datetime.datetime(year, 1, 1, tzinfo=timezone.utc)
        end_of_year_utc = datetime.datetime(year + 1, 1, 1, tzinfo=timezone.utc)

        start_ms = int(start_of_year_utc.timestamp() * 1000)
        end_ms = int(end_of_year_utc.timestamp() * 1000)

        all_flat_data = [
            d for d in all_flat_data if start_ms <= d["timestamp"] < end_ms
        ]

        weekly_energy = defaultdict(float)

        for row in all_flat_data:
            dt = datetime.datetime.fromtimestamp(
                row["timestamp"] / 1000, tz=timezone.utc
            )
            iso_year, iso_week, _ = dt.isocalendar()  # returns (year, week, weekday)
            if iso_year == year:
                weekly_energy[iso_week] += row["energy"]

        # Determine total weeks in the year
        dec28 = datetime.date(year, 12, 28)
        total_weeks = dec28.isocalendar()[1]

        # Fill missing weeks with 0.0
        data_output = [
            {"week": w, "total_energy": round(float(weekly_energy.get(w, 0.0)), 6)}
            for w in range(1, total_weeks + 1)
        ]

    # ---- 3. Store in cache ----
    # Past years are immutable → cache forever
    if year < current_year:
        await vc.set(cache_key, json.dumps(data_output))
    else:
        # Current year → still useful, but allow expiry
        await vc.set(
            cache_key,
            json.dumps(data_output),
            expiry=ExpirySet(value=(6 * 3600), expiry_type=ExpiryType.SEC),
        )

    return data_output


@m3ter_router.get(
    "/month/{year}/{month}", response_model=List[output.MonthOfYearResponse]
)
async def get_month_of_year(
    m3ter_id: int,
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
    cache_key = f"energy:{m3ter_id}:month:{year}:{month}"
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
            "meterNumber": m3ter_id,
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


@m3ter_router.get("/activities", response_model=output.ActivitiesResponse)
async def get_activities(m3ter_id: int, after: str | None = None, limit: int = 10):
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
    query.variable_values = {"meterNumber": m3ter_id, "first": limit, " after": after}

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
