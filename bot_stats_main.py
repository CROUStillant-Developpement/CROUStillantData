import asyncio

from CROUStillantData.bot_stats import BotStats
from aiohttp import ClientSession
from asyncpg import create_pool
from os import environ
from dotenv import load_dotenv


load_dotenv(dotenv_path="/CROUStillantData/.env")


async def main():
    """
    Point d'entrée du relevé des statistiques du bot Discord (voir crontab).
    Séparé de __main__.py : le relevé doit tomber à intervalle fixe (toutes
    les 5 min, jour et nuit) pour que l'uptime et les courbes soient
    comparables, et un bot lent ou injoignable ne doit pas retarder le
    rafraîchissement des vues ni StatsAggregator.
    """
    pool = await create_pool(
        database=environ["POSTGRES_DATABASE"],
        user=environ["POSTGRES_USER"],
        password=environ["POSTGRES_PASSWORD"],
        host=environ["POSTGRES_HOST"],
        port=environ["POSTGRES_PORT"],
        min_size=1,
        max_size=1,
    )

    async with ClientSession() as session:
        bot_stats = BotStats(
            pool=pool,
            session=session,
            url=environ["DPYSTATUS_URL"],
            token=environ.get("DPYSTATUS_TOKEN") or None,
        )
        await bot_stats.run()

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
