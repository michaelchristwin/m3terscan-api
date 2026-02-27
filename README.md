# m3terscan-api

REST API backend for **m3terscan**, a service that aggregates energy usage data and on-chain proposal details for m3ter meters.

---

## 📌 Overview

`m3terscan-api` is a Python web service built with **FastAPI** that exposes:

- Meter energy usage aggregates (daily, weekly, monthly, yearly)
- Recent activity for a given meter
- On-chain proposal data decoded from Ethereum

These endpoints integrate with an external GraphQL source for meter data and a Web3 Ethereum RPC for proposal data.

---

## 🚀 Features

### Meter Usage Endpoints

Built under `/meter/{meter_id}`:

| Endpoint | Description |
|---|---|
| `GET /daily` | Daily energy usage aggregates |
| `GET /weekly` | Weekly usage aggregates |
| `GET /weeks/{year}` | Energy totals per week for a specified year |
| `GET /month/{month}/{year}` | Month-by-day usage |
| `GET /activities` | Recent activity for a meter |

All endpoints return structured JSON data for easy consumption by dashboards or frontend clients.

### Proposal Endpoint

| Endpoint | Description |
|---|---|
| `GET /proposal/{tx_hash}` | Returns decoded proposal details (meter numbers, accounts, nonces) for a given transaction hash. Results are cached locally in SQLite for faster subsequent access. |

---

## 🧠 Architecture

- **FastAPI** app entrypoint: `main.py`
- **Route modules:**
  - `routes/meter.py` — Meter usage and aggregation logic
  - `routes/proposal.py` — Proposal fetching + blockchain decoding
- Database caching via **SQLite**
- **GraphQL** integrations to fetch meter data
- **Web3 Ethereum RPC** for contract calls
- **CORS middleware** enabled for known origins (e.g., dashboard clients)

---

## 📦 Installation & Running

### Requirements

- Python 3.10+
- Dependencies installed via `pyproject.toml` (Poetry/uv)

### Setup

```bash
# Clone repository
git clone https://github.com/michaelchristwin/m3terscan-api.git
cd m3terscan-api

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install .
```

### Environment

Ensure environment variables, config files, or services required by the following are set up before running the server:

- GraphQL client
- Web3 Ethereum RPC
- Valkey cache client

### Run

```bash
uvicorn main:app --reload
```

---

## 🧪 Example Usage

**Days Data**

```
GET /meter/1234/daily
```

Returns a list of daily usage aggregates for meter `1234`.

**Proposal Data**

```
GET /proposal/0xabc123def...
```

Returns structured proposal entries decoded from Ethereum.

---

## 📚 Documentation

Refer to the code for full type definitions and models returned by routes under `/models`.

---

## 🗂️ Contribution

1. Fork the repo
2. Create a feature branch
3. Submit a Pull Request
