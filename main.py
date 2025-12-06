"""
Project entry point for m3terscan API.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import m3ter, proposal

app = FastAPI()

origins = [
    "http://localhost:5174",
    "http://localhost:5173",
    "https://alliancepower.io",
    "https://ap-dashboard-kappa.vercel.app",
    "https://m3terscan.m3ter.ing",
    "https://explore.m3ter.ing",
    "https://m3terscan-rr.vercel.app",
]

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
