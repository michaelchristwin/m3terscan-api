"""
Project entry point for m3terscan API.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import m3ter, proposal
from config import valkey_client


@asynccontextmanager
async def lifespan(application: FastAPI):
    """
    Docstring for lifespan
    """
    application.title = "M3terscan API"
    await valkey_client.get_client()
    yield


origins = [
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


@app.get("/")
def read_root():
    """
    Welcome message to our beloved users.
    """
    return {"message": "Hello from the m3terscan API"}


app.include_router(m3ter.m3ter_router)
app.include_router(proposal.proposal_router)
