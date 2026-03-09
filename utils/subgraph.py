from gql import gql

from config import graphql


async def fetch_page(
    meter_id: int, min_block: int, max_block: int, cursor: str | None = None
):
    """Fetches a page of data from the GraphQL endpoint."""
    variables = {
        "meterNumber": meter_id,
        "block": {"min": min_block, "max": max_block},
        "first": 3000,  # Set as requested
        "sortBy": "HEIGHT_ASC",
        "after": cursor,
    }

    # Note: Added $first, $sortBy, and $after variables to the query definition
    query = gql(
        """
    query BaseQuery($meterNumber: Int!,
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

    # Flatten data and include the cursor for the next iteration
    return [
        {
            "timestamp": i["node"]["timestamp"],
            "energy": i["node"]["payload"]["energy"],
            "cursor": i["cursor"],
        }
        for i in items
    ]
