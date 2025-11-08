import asyncio

from CROUStillantData.worker import Worker
from CROUStillantData.analytics import Analytics
from aiohttp import ClientSession
from asyncpg import create_pool
from os import environ
from dotenv import load_dotenv


load_dotenv(dotenv_path="/CROUStillantData/.env")


async def main():
    """
    Main function
    """

    # Refresh views
    pool = await create_pool(
        database=environ["POSTGRES_DATABASE"],
        user=environ["POSTGRES_USER"],
        password=environ["POSTGRES_PASSWORD"],
        host=environ["POSTGRES_HOST"],
        port=environ["POSTGRES_PORT"],
        min_size=10,  # 10 connections
        max_size=10,  # 10 connections
        max_queries=50000,  # 50,000 queries
    )

    worker = Worker(
        pool=pool,
    )
    await worker.run()


    # Process analytics geo data
    session = ClientSession()

    analytics_pool = await create_pool(
        database=environ["ANALYTICS_POSTGRES_DATABASE"],
        user=environ["ANALYTICS_POSTGRES_USER"],
        password=environ["ANALYTICS_POSTGRES_PASSWORD"],
        host=environ["ANALYTICS_POSTGRES_HOST"],
        port=environ["ANALYTICS_POSTGRES_PORT"],
        min_size=10,  # 10 connections
        max_size=10,  # 10 connections
        max_queries=50000,  # 50,000 queries
    )

    analytics = Analytics(
        session=session,
        pool=pool,
        analytics_pool=analytics_pool,
        websites_ids=environ["WEBSITES_IDS"].split(","),
        photon_api=environ.get("PHOTON_API_URL")
    )

    await analytics.process()

    await session.close()
    await pool.close()
    await analytics_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
