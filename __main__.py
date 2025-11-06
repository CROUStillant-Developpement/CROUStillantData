import asyncio

from CROUStillantData.worker import Worker
from asyncpg import create_pool
from os import environ
from dotenv import load_dotenv


load_dotenv(dotenv_path="/CROUStillantData/.env")


async def main():
    """
    Main function
    """
    
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

if __name__ == "__main__":
    asyncio.run(main())
