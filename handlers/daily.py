"""
Route handler and helper functions for daily endpoint.
"""

import datetime
from collections import defaultdict
from datetime import timedelta, timezone
from typing import Dict, List, Optional, Tuple, Sequence
from glide import GlideClient, Batch, ExpiryType, ExpirySet
from gql import gql
from utils import latest_block
from config import valkey_client, graphql


class EnergyDataCache:
    """Handles caching of hourly energy data in Valkey using valkey-glide."""

    def __init__(self, client: GlideClient):
        self.client = client
        self.cache_ttl = 48 * 3600  # 48 hours in seconds

    def _cache_key(self, m3ter_id: int, date_str: str, hour: int) -> str:
        """Generate cache key for specific meter, date, and hour"""
        return f"energy:{m3ter_id}:{date_str}:{hour}"

    async def get_hourly_cache_batch(
        self, m3ter_id: int, date_str: str, hours: List[int]
    ) -> Dict[int, Optional[float]]:
        """
        Batch fetch cached values for multiple hours using client.mget.
        """
        if not hours:
            return {}

        keys: Sequence[str] = [
            self._cache_key(m3ter_id, date_str, hour) for hour in hours
        ]
        values: Sequence[Optional[bytes]] = await self.client.mget(keys)  # type: ignore

        result: Dict[int, Optional[float]] = {}

        for hour, val in zip(hours, values):
            if val is not None:
                # The value is bytes, decode it before converting to float
                decoded_val = val.decode("utf-8")
                try:
                    result[hour] = float(decoded_val)
                except ValueError:
                    result[hour] = None
            else:
                result[hour] = None
        return result

    async def set_hourly_cache_batch(
        self, m3ter_id: int, date_str: str, hourly_data: Dict[int, float]
    ) -> None:
        """
        Batch set cache values for multiple hours using a Glide Batch object,
        using the SET command with the expire_time argument for TTL.
        """
        if not hourly_data:
            return

        batch = Batch(is_atomic=True)

        # Queue all commands onto the Batch object
        for hour, energy in hourly_data.items():
            key = self._cache_key(m3ter_id, date_str, hour)

            batch.set(
                key=key,
                value=str(energy),
                expiry=ExpirySet(value=self.cache_ttl, expiry_type=ExpiryType.SEC),
            )

        # Execute the batch
        await self.client.exec(batch, raise_on_error=True)


class TimeBoundaryManager:
    """Manages time boundaries and hour completion logic"""

    @staticmethod
    def get_current_utc_time() -> datetime.datetime:
        """Get current UTC time"""
        return datetime.datetime.now(tz=timezone.utc)

    @staticmethod
    def get_date_hour_info(utc_time: datetime.datetime) -> Tuple[str, int, List[int]]:
        """
        Extract date string, current hour, and all hour indices
        Returns: (date_str, current_hour, [0, 1, ..., 23])
        """
        date_str = utc_time.strftime("%Y-%m-%d")
        current_hour = utc_time.hour
        all_hours = list(range(24))
        return date_str, current_hour, all_hours

    @staticmethod
    def get_completed_hours(current_hour: int) -> List[int]:
        """Get list of hours that are completed (excluding current hour)"""
        return list(range(0, current_hour))


class GraphQLDataFetcher:
    """Handles GraphQL data fetching with pagination"""

    def __init__(self, m3ter_id: int, height: int, min_block: int):
        self.m3ter_id = m3ter_id
        self.height = height
        self.min_block = min_block

    async def fetch_hourly_data(
        self, start_hour: int, end_hour: int
    ) -> Dict[int, float]:
        """
        Func
        """
        now_utc = datetime.datetime.now(tz=timezone.utc)
        start_of_day = datetime.datetime(
            year=now_utc.year,
            month=now_utc.month,
            day=now_utc.day,
            tzinfo=timezone.utc,
        )

        start_time = start_of_day + timedelta(hours=start_hour)
        end_time = start_of_day + timedelta(hours=end_hour)

        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        all_flat_data = await self._fetch_paginated_data(start_ms, end_ms)

        if not all_flat_data:
            return {}  # ✅ NOTHING, not zeros

        return self._aggregate_by_hour(all_flat_data)

    async def _fetch_paginated_data(self, start_ms: int, end_ms: int) -> List[dict]:
        """Fetch paginated data from GraphQL"""
        all_flat_data = []
        cursor = None

        while True:
            page = await self._fetch_page(cursor, start_ms, end_ms)
            if not page:
                break
            all_flat_data.extend(page)
            cursor = page[-1]["cursor"]

        return all_flat_data

    async def _fetch_page(
        self, cursor: Optional[str], start_ms: int, end_ms: int
    ) -> List[dict]:
        """Fetch a single page of data"""
        variables = {
            "meterNumber": self.m3ter_id,
            "block": {"min": self.min_block, "max": self.height},
            "first": 500,
            "sortBy": "HEIGHT_ASC",
            "after": cursor,
        }

        query = gql(
            """
        query DailyQuery($meterNumber: Int!, 
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

        try:
            result = await graphql.gql_query(query)
            items = result.get("meterDataPoints", [])

            # Filter for timestamp range
            filtered_items = []
            for i in items:
                timestamp = i["node"]["timestamp"]
                if start_ms <= timestamp < end_ms:
                    filtered_items.append(
                        {
                            "timestamp": timestamp,
                            "energy": i["node"]["payload"]["energy"],
                            "cursor": i["cursor"],
                        }
                    )

            return filtered_items
        except RuntimeError:
            return []

    def _aggregate_by_hour(self, data: List[dict]) -> Dict[int, float]:
        hourly_energy = defaultdict(float)

        for row in data:
            dt = datetime.datetime.fromtimestamp(
                row["timestamp"] / 1000, tz=timezone.utc
            )
            hour = dt.hour
            hourly_energy[hour] += row["energy"]

        # ✅ Return ONLY hours that exist
        return dict(hourly_energy)


async def get_daily_with_cache(m3ter_id: int):
    """Get daily energy usage aggregate with caching for completed hours."""
    vc = valkey_client.ValkeyManager.get_client()
    cache_manager = EnergyDataCache(vc)
    time_manager = TimeBoundaryManager()

    now_utc = time_manager.get_current_utc_time()
    date_str, current_hour, _ = time_manager.get_date_hour_info(now_utc)
    completed_hours = time_manager.get_completed_hours(current_hour)

    height, timestamp = latest_block.get_latest_block()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    # print("Max block: ", height)
    start_of_day = datetime.datetime(
        year=now_utc.year,
        month=now_utc.month,
        day=now_utc.day,
        tzinfo=timezone.utc,
    )
    block_interval = timedelta(minutes=2)
    min_block = int(height - (timestamp - start_of_day) / block_interval)
    # print("Min Block: ", min_block)
    result: Dict[int, float] = {}

    # --- Read cache (completed hours only, no defaults)
    cached_data = {}
    if completed_hours:
        cached_data = await cache_manager.get_hourly_cache_batch(
            m3ter_id, date_str, completed_hours
        )

        for hour, energy in cached_data.items():
            if energy is not None:
                result[hour] = energy

    # --- Determine hours to fetch
    missing_completed = [h for h in completed_hours if cached_data.get(h) is None]

    hours_to_fetch = set(missing_completed)
    hours_to_fetch.add(current_hour)

    fresh_data = {}

    if hours_to_fetch:
        fetcher = GraphQLDataFetcher(m3ter_id, height, min_block)

        hours = sorted(hours_to_fetch)
        ranges = []
        start = end = hours[0]

        for h in hours[1:]:
            if h == end + 1:
                end = h
            else:
                ranges.append((start, end + 1))
                start = end = h
        ranges.append((start, end + 1))

        for start_hour, end_hour in ranges:
            data = await fetcher.fetch_hourly_data(start_hour, end_hour)
            fresh_data.update(data)

    # --- Merge fresh data
    for hour, energy in fresh_data.items():
        result[hour] = energy

    # --- Cache completed hours only
    completed_fresh = {
        hour: energy for hour, energy in fresh_data.items() if hour in completed_hours
    }
    if completed_fresh:
        await cache_manager.set_hourly_cache_batch(m3ter_id, date_str, completed_fresh)

    if not result:
        return []

    return [
        {
            "hour_start_utc": (start_of_day + timedelta(hours=hour))
            .isoformat()
            .replace("+00:00", "Z"),
            "total_energy": energy,
        }
        for hour, energy in sorted(result.items())
    ]


# Fallback function without cache (for error handling)
async def get_daily_without_cache(m3ter_id: int):
    """
    Fallback function that uses the original implementation without caching.
    Use this if cache is unavailable or there are cache errors.
    """
    height, timestamp = latest_block.get_latest_block()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    now = datetime.datetime.now(tz=timezone.utc)
    start_of_day = datetime.datetime(
        year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc
    )
    block_interval = timedelta(minutes=2)
    min_block = int(height - (timestamp - start_of_day) / block_interval)

    async def fetch_page(cursor: str | None = None):
        variables = {
            "meterNumber": m3ter_id,
            "block": {"min": min_block, "max": height},
            "first": 500,
            "sortBy": "HEIGHT_ASC",
            "after": cursor,
        }

        query = gql(
            """
            query DailyQuery(
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

    if not all_flat_data:
        return []

    start_of_day_utc = start_of_day
    end_of_day_utc = start_of_day_utc + timedelta(days=1)

    hourly_energy = defaultdict(float)

    for item in all_flat_data:
        ts = item["timestamp"]
        if not (
            start_of_day_utc.timestamp() * 1000
            <= ts
            < end_of_day_utc.timestamp() * 1000
        ):
            continue

        dt = datetime.datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        hour_dt = dt.replace(minute=0, second=0, microsecond=0)
        hourly_energy[hour_dt] += item["energy"]

    return [
        {
            "hour_start_utc": hour.isoformat().replace("+00:00", "Z"),
            "total_energy": float(energy),
        }
        for hour, energy in sorted(hourly_energy.items())
    ]
