"""
Models for API response.
"""

from typing import List
from pydantic import BaseModel


class DailyResponse(BaseModel):
    """
    Response model for daily enpoint.
    """

    hour_start_utc: str
    total_energy: float


class WeeklyResponse(BaseModel):
    """
    Response model for weekly enpoint.
    """

    day_of_week: int
    total_energy: float


class WeeksOfYearResponse(BaseModel):
    """
    Response model to get weeks of a year.
    """

    week: int
    total_energy: float


class MonthOfYearResponse(BaseModel):
    """
    Response model to get a specified month of a specified year.
    """

    day: int
    total_energy: float


class Activities(BaseModel):
    """
    Docstring for Activities
    """

    timestamp: int
    energy: float
    signature: str


class ActivitiesResponse(BaseModel):
    """
    Docstring for ActivitiesResponse
    """

    data: List[Activities]
    limit: int
    next_cursor: str


class ProposalsResponse(BaseModel):
    """
    Represents the structured response containing a meter number,
    a formatted account ID, and a nonce value.
    """

    m3ter_no: int
    account: str
    nonce: int
