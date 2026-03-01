"""
Project entry point for m3terscan API.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlmodel import SQLModel

from config import valkey_client
from database import engine
from models.monthly import MonthlyEnergy
from models.weeks_of_year import WeeksEnergy
from routes import meter, proposal


@asynccontextmanager
async def lifespan(application: FastAPI):
    """
    Docstring for lifespan
    """
    application.title = "M3terscan API"
    SQLModel.metadata.create_all(engine)
    await valkey_client.ValkeyManager.init()
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
    return RedirectResponse(url="/static/favicon.ico")


@app.get("/")
def read_root():
    """
    Welcome message to our users.
    """
    return {"message": "Hello M3terheads 😎"}


app.include_router(meter.meter_router)
app.include_router(proposal.proposal_router)
