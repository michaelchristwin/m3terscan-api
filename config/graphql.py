"""
Configuration for m3ters graphql endpoint.
"""

from gql import Client, GraphQLRequest
from gql.transport.aiohttp import AIOHTTPTransport


async def gql_query(query: GraphQLRequest):
    """
    Graphql query helper.
    """
    transport = AIOHTTPTransport(url="https://subgraph.m3ter.ing/v2")

    async with Client(
        transport=transport,
        fetch_schema_from_transport=True,
    ) as client:
        return await client.execute(query)
