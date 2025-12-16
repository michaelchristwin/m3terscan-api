"""
Route handler and helper functions for daily endpoint.
"""

import datetime
from datetime import timedelta, timezone
from typing import Dict, List, Optional, Tuple, Sequence
import pandas as pd
from glide import GlideClient, Batch, ExpiryType, ExpirySet
from gql import gql
from utils import latest_block
from config import valkey_client
from config import graphql


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
        Fetch energy data for a specific hour range
        Returns: Dict of {hour: total_energy}
        """
        now_utc = datetime.datetime.now(tz=timezone.utc)
        start_of_day = datetime.datetime(
            year=now_utc.year, month=now_utc.month, day=now_utc.day, tzinfo=timezone.utc
        )

        # Calculate timestamp boundaries for the hour range
        start_time = start_of_day + timedelta(hours=start_hour)
        end_time = start_of_day + timedelta(hours=end_hour)

        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        # Fetch all data points for the time range
        all_flat_data = await self._fetch_paginated_data(start_ms, end_ms)

        if not all_flat_data:
            return {hour: 0.0 for hour in range(start_hour, end_hour)}

        # Process and aggregate by hour
        return self._aggregate_by_hour(
            all_flat_data, start_of_day, start_hour, end_hour
        )

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

    def _aggregate_by_hour(
        self,
        data: List[dict],
        start_of_day: datetime.datetime,
        start_hour: int,
        end_hour: int,
    ) -> Dict[int, float]:
        """Aggregate data by hour within the specified range"""
        if not data:
            return {hour: 0.0 for hour in range(start_hour, end_hour)}

        df = pd.DataFrame(data)
        df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize(
            "UTC"
        )
        df = df.set_index("datetime_utc")

        # Resample by hour and sum energy
        hourly = df["energy"].resample("h").sum()

        # Create result dictionary for the requested hour range
        result = {}
        for hour in range(start_hour, end_hour):
            hour_time = start_of_day + timedelta(hours=hour)
            if hour_time in hourly.index:
                result[hour] = float(hourly[hour_time])
            else:
                result[hour] = 0.0

        return result


async def get_daily_with_cache(m3ter_id: int):
    """
    Get daily energy usage aggregate with caching for completed hours.
    """
    # Initialize managers
    vc = await valkey_client.get_client()
    cache_manager = EnergyDataCache(vc)
    time_manager = TimeBoundaryManager()

    # Get time boundaries
    now_utc = time_manager.get_current_utc_time()
    date_str, current_hour, all_hours = time_manager.get_date_hour_info(now_utc)
    completed_hours = time_manager.get_completed_hours(current_hour)

    # Step 1: Get block information (same as original)
    height, timestamp = latest_block.get_latest_block()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    now = datetime.datetime.now(tz=timezone.utc)
    start_of_day = datetime.datetime(
        year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc
    )
    block_interval = timedelta(minutes=2)
    min_block = int(height - (timestamp - start_of_day) / block_interval)

    # Step 2: Check cache for completed hours
    cached_data = {}
    missing_hours = []

    if completed_hours:
        cached_data = await cache_manager.get_hourly_cache_batch(
            m3ter_id, date_str, completed_hours
        )

        # Identify hours missing from cache
        missing_hours = [hour for hour in completed_hours if cached_data[hour] is None]

    # Step 3: Prepare result dictionary
    result: Dict[int, Optional[float]] = {hour: None for hour in all_hours}

    # Step 4: Add cached values to result
    for hour in completed_hours:
        if cached_data[hour] is not None:
            result[hour] = cached_data[hour]

    # Step 5: Handle future hours (beyond current hour) - always 0.0
    for hour in range(current_hour + 1, 24):
        result[hour] = 0.0

    # Step 6: Fetch missing and current hour data
    fresh_data = {}
    hours_to_fetch = []

    # Add missing completed hours
    if missing_hours:
        hours_to_fetch.extend(missing_hours)

    # Always fetch current hour fresh
    hours_to_fetch.append(current_hour)

    if hours_to_fetch:
        fetcher = GraphQLDataFetcher(m3ter_id, height, min_block)

        # Group contiguous hours for efficient fetching
        hours_to_fetch.sort()
        hour_ranges = []
        start = hours_to_fetch[0]
        end = hours_to_fetch[0]

        for hour in hours_to_fetch[1:]:
            if hour == end + 1:
                end = hour
            else:
                hour_ranges.append((start, end + 1))
                start = hour
                end = hour
        hour_ranges.append((start, end + 1))

        # Fetch data for each range
        for start_hour, end_hour in hour_ranges:
            range_data = await fetcher.fetch_hourly_data(start_hour, end_hour)
            fresh_data.update(range_data)

    # Step 7: Update result with fresh data
    for hour, energy in fresh_data.items():
        result[hour] = energy

    # Step 8: Cache newly fetched completed hours (excluding current hour)
    if fresh_data:
        completed_fresh_data = {
            hour: energy
            for hour, energy in fresh_data.items()
            if hour in completed_hours
        }
        if completed_fresh_data:
            await cache_manager.set_hourly_cache_batch(
                m3ter_id, date_str, completed_fresh_data
            )

    # Step 9: Fill in any remaining None values with 0.0 (shouldn't happen but safety)
    for hour in all_hours:
        if result[hour] is None:
            result[hour] = 0.0

    # Step 10: Format and return result
    start_of_day_utc = datetime.datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    return [
        {
            "hour_start_utc": (start_of_day_utc + timedelta(hours=hour))
            .isoformat()
            .replace("+00:00", "Z"),
            "total_energy": result[hour],
        }
        for hour in all_hours
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

    # Difference in number of blocks since start of day
    min_block = int(height - (timestamp - start_of_day) / block_interval)

    async def fetch_page(cursor: str | None = None):
        variables = {
            "meterNumber": m3ter_id,
            "block": {"min": min_block, "max": height},
            "first": 500,
            "sortBy": "HEIGHT_ASC",
            "after": None,
        }
        if cursor:
            variables["after"] = cursor

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
        now_utc = pd.Timestamp.now(tz="UTC").normalize()
        full_index = pd.date_range(start=now_utc, periods=24, freq="h", tz="UTC")

        return [
            {
                "hour_start_utc": ts.isoformat().replace("+00:00", "Z"),
                "total_energy": 0.0,
            }
            for ts in full_index
        ]
    start_of_day_utc = datetime.datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_of_day_utc = start_of_day_utc + datetime.timedelta(days=1)

    start_ms = int(start_of_day_utc.timestamp() * 1000)
    end_ms = int(end_of_day_utc.timestamp() * 1000)

    # Sanitize: keep only today's data
    all_flat_data = [
        item for item in all_flat_data if start_ms <= item["timestamp"] < end_ms
    ]

    df = pd.DataFrame(all_flat_data)
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize(
        "UTC"
    )
    df = df.set_index("datetime_utc")

    hourly = df["energy"].resample("h").sum()

    start_of_day_utc = hourly.index.min().floor("D")
    full_day_index = pd.date_range(
        start=start_of_day_utc, periods=24, freq="h", tz="UTC"
    )

    hourly = hourly.reindex(full_day_index, fill_value=0.0)

    return [
        {
            "hour_start_utc": ts.isoformat().replace("+00:00", "Z"),  # type: ignore
            "total_energy": float(energy),
        }
        for ts, energy in hourly.items()
    ]
