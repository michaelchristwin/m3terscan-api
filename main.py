"""
Project entry point for m3terscan API.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Annotated, Any

import httpx
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.types import DuneRecord
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlmodel import SQLModel

from config import valkey_client
from database import engine
from dune.run_query import run_dune_query
from handlers.daily import get_daily_with_cache
from models.monthly import MonthlyEnergy  # noqa: F401
from models.weeks_of_year import WeeksEnergy  # noqa: F401
from routes import meter, proposal

load_dotenv(dotenv_path=".env")


@asynccontextmanager
async def lifespan(application: FastAPI):
    """
    Docstring for lifespan
    """
    application.title = "M3terscan API"
    SQLModel.metadata.create_all(engine)
    await valkey_client.ValkeyManager.init()
    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        raise RuntimeError("DUNE_API_KEY environment variable is not set")

    # Sync SDK client — fine to keep around, just don't call its blocking
    # methods directly in async routes (wrap with asyncio.to_thread)
    app.state.dune = DuneClient(api_key)

    # Async HTTP client for your custom fast-path queries
    app.state.dune_http = httpx.AsyncClient(
        base_url="https://api.dune.com/api/v1",
        headers={"X-DUNE-API-KEY": api_key},
        timeout=10.0,
    )
    yield
    await valkey_client.ValkeyManager.close()


origins = [
    "http://localhost:3000",
    "http://localhost:5174",
    "http://localhost:5173",
    "https://alliancepower.io",
    "https://ap-dashboard-kappa.vercel.app",
    "https://m3terscan.m3ter.ing",
    "https://explore.m3ter.ing",
    "https://m3terscan-rr.vercel.app",
    "https://m3terstate-diff.pages.dev",
]

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """
    Returns favicon
    """
    return RedirectResponse(url="/static/favicon.ico")


@app.get("/")
def read_root():
    """
    Welcome message to our users.
    """
    return {"message": "Hello M3terheads 😎"}


@app.get("/recent-blocks")
async def get_recent_blocks(request: Request) -> list[DuneRecord]:
    """
    Get latest blocks
    """

    dune = request.app.state.dune
    result = dune.get_latest_result(query=5911866)
    return result.result.rows  # type: ignore


@app.post("/recent-blocks")
async def execute_recent_blocks(request: Request):
    """
    Execute query for recent blocks on dune
    """
    dune_http = request.app.state.dune_http
    return await run_dune_query(
        dune_http=dune_http, query_id=5911866, performance="medium"
    )


@app.get("/world-state")
async def get_world_state() -> list[DuneRecord]:
    """
    Lorem ipsum
    """
    dune_api_key = os.getenv("DUNE_API_KEY")
    dune = DuneClient(dune_api_key)
    result = dune.get_latest_result(query=5933916)
    return result.result.rows  # type: ignore


@app.get("/daily-batch")
async def get_daily_batch(
    meter_ids: Annotated[
        list[int], Query(description="Repeat param: ?meter_ids=1&meter_ids=2")
    ],
) -> dict[str, Any]:
    """
    Get daily Batch
    """

    async def run_one(meter_id: int):
        data = await get_daily_with_cache(meter_id)
        return meter_id, data

    results = await asyncio.gather(*(run_one(mid) for mid in meter_ids))

    return {str(meter_id): data for meter_id, data in results}


app.include_router(meter.meter_router)
app.include_router(proposal.proposal_router)
