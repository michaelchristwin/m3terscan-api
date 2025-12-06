"""
APIRouter module for m3ter endpoint.
"""

import datetime
import calendar
from datetime import timedelta, timezone
from typing import List
import pandas as pd
from gql import gql
from fastapi import APIRouter
from utils import latest_block
from models import output
from config import graphql


m3ter_router = APIRouter(prefix="/m3ter/{m3ter_id}")


@m3ter_router.get("/daily", response_model=List[output.DailyResponse])
async def get_daily(m3ter_id: int):
    """
    Get daily energy usage aggregate.
    """
    height, timestamp = latest_block.get_latest_block()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    now = datetime.datetime.now(timezone.utc)
    start_of_day = datetime.datetime(
        year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc
    )
    block_interval = timedelta(minutes=2)

    # Difference in number of blocks since start of day
    min_block = int(height - (timestamp - start_of_day) / block_interval)
    query = gql(
        """
        query DailyQuery($meterNumber: Int!, $block: BlockFilter) {
            meterDataPoints(meterNumber: $meterNumber, block: $block) {
                node {
                    timestamp
                    payload {
                        energy
                    }
                }
            }
        }
        """
    )
    query.variable_values = {
        "meterNumber": m3ter_id,
        "block": {"min": min_block, "max": height},
    }

    result = await graphql.gql_query(query)
    meter_data_points = result.get("meterDataPoints", [])

    # 1. Extract and flatten the required fields
    flat_data = [
        {
            "timestamp": item["node"]["timestamp"],
            "energy": item["node"]["payload"]["energy"],
        }
        for item in meter_data_points
    ]

    # Handle the empty array case:
    if not flat_data:
        # Create a full 24-hour UTC index for a default date (e.g., today's date)
        # We need a starting date to generate the ISO strings. Using the current time.
        now_utc = pd.Timestamp.now(tz="UTC").normalize()
        full_index = pd.date_range(start=now_utc, periods=24, freq="h", tz="UTC")

        return [
            {
                "hour_start_utc": ts.isoformat().replace("+00:00", "Z"),
                "total_energy": 0.0,
            }
            for ts in full_index
        ]

    # 2. Create DataFrame and convert millisecond timestamp to UTC datetime
    df = pd.DataFrame(flat_data)

    # CRITICAL: Convert ms to datetime, localize it to UTC immediately.
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize(
        "UTC"
    )

    # 3. Set the datetime column as the index for resampling
    df = df.set_index("datetime_utc")

    # 4. Resample to Hourly (H) frequency, aggregate by sum, and fill missing hours with 0
    # .resample('H') groups data into precise hourly bins.
    # .sum() aggregates the energy within that bin.
    # .fillna(0.0) ensures every hour has a value.
    hourly_aggregation = df["energy"].resample("h").sum().fillna(0.0)

    # 5. Handle all 24 hours (Reindexing to a fixed 24-hour period)
    # This step ensures the output array is exactly 24 items, covering a full day
    # relative to the first piece of data.

    # Determine the start of the first day present in the data
    start_of_day_utc = hourly_aggregation.index.min().floor("D")

    # Create a continuous 24-hour index spanning that day (00:00:00 to 23:00:00)
    full_day_index = pd.date_range(
        start=start_of_day_utc, periods=24, freq="h", tz="UTC"
    )

    # Reindex the aggregation to fit the full 24-hour day
    hourly_filled = hourly_aggregation.reindex(full_day_index, fill_value=0.0)

    # 6. Format the output to a list of dictionaries with the ISO 8601 string
    output_array = [
        {
            # Format as ISO 8601 string: YYYY-MM-DDTHH:MM:SSZ
            "hour_start_utc": ts.isoformat().replace("+00:00", "Z"),  # type: ignore
            "total_energy": round(energy, 6),
        }
        for ts, energy in hourly_filled.items()
    ]

    return output_array


@m3ter_router.get("/weekly", response_model=List[output.WeeklyResponse])
async def get_weekly(m3ter_id: int):
    """
    Get weekly energy usage aggregate.
    """
    height, _ = latest_block.get_latest_block()
    interval = datetime.datetime.now().weekday() * 720
    min_block = height - interval
    query = gql(
        """
        query DailyQuery($meterNumber: Int!, $block: BlockFilter) {
            meterDataPoints(meterNumber: $meterNumber, block: $block) {
                node {
                    timestamp
                    payload {
                        energy
                    }
                }
            }
        }
        """
    )
    query.variable_values = {
        "meterNumber": m3ter_id,
        "block": {"min": min_block, "max": height},
    }

    result = await graphql.gql_query(query)

    meter_data_points = result.get("meterDataPoints", [])

    # 1. Extract and flatten the required fields
    flat_data = [
        {
            "timestamp": item["node"]["timestamp"],
            "energy": item["node"]["payload"]["energy"],
        }
        for item in meter_data_points
    ]

    # Handle the empty array case:
    if not flat_data:
        # If no data points, return 7 days of 0 energy immediately.
        return [{"day_of_week": i, "total_energy": 0.0} for i in range(7)]

    # 2. Create DataFrame and convert millisecond timestamp to datetime
    df = pd.DataFrame(flat_data)

    # CRITICAL: Convert ms to datetime, localize it to UTC for consistency.
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize(
        "UTC"
    )

    # 3. Extract the day of the week: Monday=0, Sunday=6
    df["day_of_week"] = df["datetime_utc"].dt.dayofweek  # type: ignore

    # 4. Group by day of the week and sum the energy
    # The result is a Pandas Series indexed by the day number (0 to 6)
    daily_aggregation = df.groupby("day_of_week")["energy"].sum()

    # 5. Reindex to include all 7 days (0 to 6) and fill missing values with 0
    full_days = pd.Index(range(7), name="day_of_week")
    daily_filled = daily_aggregation.reindex(full_days, fill_value=0.0)

    # 6. Format the output to a list of dictionaries
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

     :param m3ter_id: Description
     :type m3ter_id: int
     :param year: Description
     :type year: int
    """
    height, _ = latest_block.get_latest_block()
    today = datetime.date.today()
    current_year = datetime.datetime.now(
        timezone.utc
    ).year  # or today.year if you prefer local

    blocks_per_week = 5040  # 720 blocks/day * 7

    if year > current_year:
        raise ValueError("Cannot calculate future year blocks")

    # --- Calculate total weeks in the target year ---
    if year == current_year:
        # Current year: only count completed weeks so far
        weeks_in_year = today.isocalendar()[1]  # ISO week number
    else:
        # Past full year: use the reliable Dec 28 trick
        dec28 = datetime.date(year, 12, 28)
        weeks_in_year = dec28.isocalendar()[1]  # always 52 or 53

    total_blocks_in_year = weeks_in_year * blocks_per_week

    # --- Calculate blocks from previous years ---
    blocks_before_year = 0
    for y in range(2024, year):  # assuming genesis/start 2024, adjust if needed
        dec28 = datetime.date(y, 12, 28)
        weeks = dec28.isocalendar()[1]
        blocks_before_year += weeks * blocks_per_week

    min_block = blocks_before_year + 1
    max_block = blocks_before_year + total_blocks_in_year

    # --- Special case: current year → cap at current height ---
    if year == current_year:
        max_block = height
    query = gql(
        """
        query DailyQuery($meterNumber: Int!, $block: BlockFilter) {
            meterDataPoints(meterNumber: $meterNumber, block: $block) {
                node {
                    timestamp
                    payload {
                        energy
                    }
                }
            }
        }
        """
    )
    query.variable_values = {
        "meterNumber": m3ter_id,
        "block": {"min": min_block, "max": max_block},
    }

    result = await graphql.gql_query(query)
    meter_data_points = result.get("meterDataPoints", [])

    # 1. Extract and flatten the required fields
    flat_data = [
        {
            "timestamp": item["node"]["timestamp"],
            "energy": item["node"]["payload"]["energy"],
        }
        for item in meter_data_points
    ]

    # Handle the empty array case:
    if not flat_data:
        # If empty, assume the current year (2025) which has 52 weeks.
        return [{"week": i, "total_energy": 0.0} for i in range(1, 53)]

    # 2. Create DataFrame and convert ms timestamp to UTC datetime
    df = pd.DataFrame(flat_data)
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize(
        "UTC"
    )

    # 3. Extract the ISO week number (1 to 52/53) and Year
    df["iso_week"] = df["datetime_utc"].dt.isocalendar().week  # type: ignore
    df["year"] = df["datetime_utc"].dt.year  # type:ignore

    # 4. Group by year and week number and sum the energy
    weekly_aggregation = df.groupby(["year", "iso_week"])["energy"].sum().reset_index()

    # --- Step 5: Determine the full week index for the specific year ---

    # Use the year of the last timestamp as the target year for the full array length
    target_year = df["datetime_utc"].max().year

    # Robustly calculate total weeks: Dec 28th is guaranteed to be in the last week (52 or 53)
    last_week_day = pd.Timestamp(f"{target_year}-12-28", tz="UTC")
    total_weeks = last_week_day.isocalendar()[1]

    # Create a full index of weeks (1 to total_weeks)
    full_weeks_index = pd.Index(range(1, total_weeks + 1), name="iso_week")

    # Filter the aggregation to only include the target year's weeks
    yearly_agg = weekly_aggregation[
        weekly_aggregation["year"] == target_year
    ].set_index("iso_week")

    # 6. Reindex and fill missing weeks with 0.0
    weekly_filled = yearly_agg["energy"].reindex(full_weeks_index, fill_value=0.0)

    # 7. Format the output to a list of dictionaries
    output_array = [
        {"week": week, "total_energy": round(energy, 6)}
        for week, energy in weekly_filled.items()
    ]

    return output_array


@m3ter_router.get(
    "/month/{year}/{month}", response_model=List[output.MonthOfYearResponse]
)
async def get_month_of_year(
    m3ter_id: int,
    year: int,
    month: int,
):
    """
    Get energy usage of a month in specified year.

     :param m3ter_id: Description
     :type m3ter_id: int
     :param year: Description
     :type year: int
     :param month: Description
     :type month: int
    """
    height, _ = latest_block.get_latest_block()
    today = datetime.date.today()
    target = datetime.date(year, month, 1)
    interval = (target - today).days * 720
    min_block = height - interval
    last_day = calendar.monthrange(year, month)[1]
    interval_forward = (last_day - 1) * 720
    max_block = min(min_block + interval_forward, height)
    query = gql(
        """
        query DailyQuery($meterNumber: Int!, $block: BlockFilter) {
            meterDataPoints(meterNumber: $meterNumber, block: $block) {
                node {
                    timestamp
                    payload {
                        energy
                    }
                }
            }
        }
        """
    )
    query.variable_values = {
        "meterNumber": m3ter_id,
        "block": {"min": min_block, "max": max_block},
    }

    result = await graphql.gql_query(query)
    meter_data_points = result.get("meterDataPoints", [])

    # ------------------------------------------------------------------
    # 3. PANDAS AGGREGATION AND ZERO-FILLING
    # ------------------------------------------------------------------

    # Extract and flatten the required fields
    flat_data = [
        {
            "timestamp": item["node"]["timestamp"],
            "energy": item["node"]["payload"]["energy"],
        }
        for item in meter_data_points
    ]

    # Handle the empty array case:
    if not flat_data:
        # If empty, return an array of 0 energy for every day in the month
        return [{"day": i, "total_energy": 0.0} for i in range(1, last_day + 1)]

    # Create DataFrame and convert ms timestamp to UTC datetime
    df = pd.DataFrame(flat_data)
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize(
        "UTC"
    )

    # Extract the day of the month (1 to 31)
    df["day_of_month"] = df["datetime_utc"].dt.day  # type: ignore

    # Group by the day of the month and sum the energy
    daily_aggregation = df.groupby("day_of_month")["energy"].sum()

    # Create a full index of days (1 to total_days: 28, 29, 30, or 31)
    full_days_index = pd.Index(range(1, last_day + 1), name="day_of_month")

    # Reindex and fill missing days with 0.0
    daily_filled = daily_aggregation.reindex(full_days_index, fill_value=0.0)

    # Format the output to a list of dictionaries
    output_array = [
        {"day": day, "total_energy": round(energy, 6)}
        for day, energy in daily_filled.items()
    ]

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
