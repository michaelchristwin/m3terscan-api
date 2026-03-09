"""
APIRouter module for meter endpoint.
"""

import datetime
from collections import defaultdict
from datetime import timezone
from typing import List

from fastapi import APIRouter
from gql import gql
from sqlmodel import Session, select

from config import graphql
from database import engine
from handlers import daily
from models.monthly import MonthlyEnergy
from models.output import (
    ActivitiesResponse,
    DailyResponse,
    WeeklyResponse,
)
from models.weeks_of_year import WeeksEnergy
from schemas.monthly import MonthlyEnergyRead
from schemas.weeks_of_year import WeeksEnergyRead
from utils import latest_block, subgraph

meter_router = APIRouter(prefix="/meter/{meter_id}", tags=["meter"])


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
                "meter_id": meter_id,
            }
            for w in range(1, total_weeks_in_year(year) + 1)
        ]

    height, _ = latest_block.get_latest_block()

    with Session(engine) as session:
        statement = select(WeeksEnergy).where(
            (WeeksEnergy.year == year) & (WeeksEnergy.meter_id == meter_id)
        )
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
                page = await subgraph.fetch_page(meter_id, min_block, max_block, cursor)
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

            records = [WeeksEnergy(meter_id=meter_id, **item) for item in data_output]
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
                page = await subgraph.fetch_page(min_block, height, height, cursor)
                if not page:
                    break
                all_data.extend(page)
                cursor = page[-1]["cursor"]

            if all_data:
                weekly = aggregate_weekly(all_data)
                current_week_energy = round(float(weekly.get(week_number, 0.0)), 6)

                stmt = select(WeeksEnergy).where(
                    WeeksEnergy.year == year,
                    WeeksEnergy.week == week_number,
                    WeeksEnergy.meter_id == meter_id,
                )

                existing = session.exec(stmt).one_or_none()

                if existing:
                    existing.total_energy = current_week_energy
                else:
                    session.add(
                        WeeksEnergy(
                            meter_id=meter_id,
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
                "meter_id": r.meter_id,
            }
            for r in db_results
        ]


@meter_router.get("/month/{month}/{year}", response_model=List[MonthlyEnergyRead])
async def get_month_of_year(
    meter_id: int,
    year: int,
    month: int,
):
    """
    Get daily energy usage for a given month in a specified year.
    """
    genesis_year = 2025
    blocks_per_day = 720  # 1 block every 2 minutes

    if year < genesis_year:
        raise ValueError(f"No data exists before {genesis_year}")

    now = datetime.datetime.now(timezone.utc)
    today = now.date()
    current_year = now.year

    target = datetime.date(year, month, 1)

    if target > today.replace(day=1):
        raise ValueError("Future month not allowed")

    if year > current_year:
        raise ValueError("Cannot calculate future year blocks")

    def month_range(y, m):
        start = datetime.date(y, m, 1)
        if m == 12:
            end = datetime.date(y + 1, 1, 1)
        else:
            end = datetime.date(y, m + 1, 1)
        return start, end

    def days_in_month(y, m):
        start, end = month_range(y, m)
        return (end - start).days

    height, _ = latest_block.get_latest_block()

    with Session(engine) as session:
        stmt = select(MonthlyEnergy).where(
            (MonthlyEnergy.year == year)
            & (MonthlyEnergy.month == month)
            & (MonthlyEnergy.metadata == meter_id)
        )

        db_results = session.exec(stmt).all()

        # --------------------------------------------------
        # INITIAL POPULATION
        # --------------------------------------------------
        if not db_results:
            month_start, month_end = month_range(year, month)

            if year == current_year and month == today.month:
                end_date = today
            else:
                end_date = month_end - datetime.timedelta(days=1)

            days_total = (end_date - month_start).days + 1
            days_after = (today - end_date).days

            if year == current_year and month == today.month:
                min_block = height - ((today - month_start).days * blocks_per_day)
                max_block = height
            else:
                max_block = height - (days_after * blocks_per_day)
                min_block = max_block - (days_total * blocks_per_day)

            all_data = []
            cursor = None

            while True:
                page = await subgraph.fetch_page(meter_id, min_block, max_block, cursor)
                if not page:
                    break
                all_data.extend(page)
                cursor = page[-1]["cursor"]

            daily_energy = defaultdict(float)

            if all_data:
                start_ms = int(
                    datetime.datetime(year, month, 1, tzinfo=timezone.utc).timestamp()
                    * 1000
                )
                end_ms = int(
                    datetime.datetime(
                        month_end.year,
                        month_end.month,
                        month_end.day,
                        tzinfo=timezone.utc,
                    ).timestamp()
                    * 1000
                )

                filtered = [d for d in all_data if start_ms <= d["timestamp"] < end_ms]

                for row in filtered:
                    dt = datetime.datetime.fromtimestamp(
                        row["timestamp"] / 1000, tz=timezone.utc
                    )
                    if dt.year == year and dt.month == month:
                        daily_energy[dt.day] += row["energy"]

            output: List[MonthlyEnergy] = []
            total_days = days_in_month(year, month)

            for day in range(1, total_days + 1):
                energy = round(float(daily_energy.get(day, 0.0)), 6)
                output.append(
                    MonthlyEnergy(
                        meter_id=meter_id,
                        year=year,
                        month=month,
                        day=day,
                        total_energy=energy,
                    )
                )

            session.add_all(output)
            session.commit()

            return [
                {
                    "year": r.year,
                    "month": r.month,
                    "day": r.day,
                    "meter_id": r.meter_id,
                    "total_energy": r.total_energy,
                }
                for r in output
            ]

        # --------------------------------------------------
        # CURRENT MONTH UPDATE (update only today)
        # --------------------------------------------------
        if year == current_year and month == today.month:
            month_start, _ = month_range(year, month)

            seconds_since_start = (
                now
                - datetime.datetime.combine(
                    month_start, datetime.time.min, tzinfo=timezone.utc
                )
            ).total_seconds()

            blocks_since_start = int(seconds_since_start / 120)
            min_block = height - blocks_since_start

            all_data = []
            cursor = None

            while True:
                page = await subgraph.fetch_page(meter_id, min_block, height, cursor)
                if not page:
                    break
                all_data.extend(page)
                cursor = page[-1]["cursor"]

            if all_data:
                today_energy = 0.0

                for row in all_data:
                    dt = datetime.datetime.fromtimestamp(
                        row["timestamp"] / 1000, tz=timezone.utc
                    )
                    if dt.year == year and dt.month == month and dt.day == today.day:
                        today_energy += row["energy"]

                today_energy = round(float(today_energy), 6)

                stmt = select(MonthlyEnergy).where(
                    MonthlyEnergy.year == year,
                    MonthlyEnergy.month == month,
                    MonthlyEnergy.day == today.day,
                    WeeksEnergy.meter_id == meter_id,
                )

                existing = session.exec(stmt).one_or_none()

                if existing:
                    existing.total_energy = today_energy
                    session.commit()

            db_results = session.exec(stmt).all()

        return [
            {
                "year": r.year,
                "month": r.month,
                "day": r.day,
                "total_energy": r.total_energy,
                "meter_id": r.meter_id,
            }
            for r in db_results
        ]


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
