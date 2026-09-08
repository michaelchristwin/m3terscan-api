import asyncio

from fastapi import HTTPException
from httpx import AsyncClient

TERMINAL_STATES = {
    "QUERY_STATE_COMPLETED",
    "QUERY_STATE_COMPLETED_PARTIAL",
    "QUERY_STATE_FAILED",
    "QUERY_STATE_CANCELLED",
    "QUERY_STATE_EXPIRED",
}


async def run_dune_query(
    dune_http: AsyncClient,
    query_id: int,
    performance: str = "medium",
    params: dict | None = None,
    poll_interval: float = 1.0,
    max_wait: float = 120.0,
) -> dict:
    # 1. Kick off execution
    resp = await dune_http.post(
        f"/query/{query_id}/execute",
        json={
            "performance": performance,
            **({"query_parameters": params} if params else {}),
        },
    )
    resp.raise_for_status()
    execution_id = resp.json()["execution_id"]

    # 2. Poll status until terminal, without blocking the event loop
    elapsed = 0.0
    while elapsed < max_wait:
        status_resp = await dune_http.get(f"/execution/{execution_id}/status")
        status_resp.raise_for_status()
        state = status_resp.json()["state"]

        if state in TERMINAL_STATES:
            if state != "QUERY_STATE_COMPLETED":
                raise HTTPException(
                    status_code=502,
                    detail=f"Dune execution ended in state {state}",
                )
            break

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
    else:
        raise HTTPException(status_code=504, detail="Dune query timed out")

    # 3. Fetch results
    results_resp = await dune_http.get(f"/execution/{execution_id}/results")
    results_resp.raise_for_status()
    return results_resp.json()
