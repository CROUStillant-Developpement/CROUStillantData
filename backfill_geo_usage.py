import asyncio

from CROUStillantData.analytics import Analytics
from aiohttp import ClientSession
from asyncpg import create_pool
from os import environ
from dotenv import load_dotenv


load_dotenv(dotenv_path="/CROUStillantData/.env")


async def main():
    """
    Seed unique de GEO_USAGE a partir de tout l'historique de la table
    analytics "session" (voir Analytics.backfill). A lancer manuellement,
    une seule fois (ou apres un reset volontaire de GEO_USAGE) :

        python backfill_geo_usage.py

    Separe de __main__.py comme archive_main.py : ce n'est pas une tache
    planifiee, juste un outil de maintenance ponctuel. GEO_DATA et GEO_USAGE
    vivent dans la base principale (POSTGRES_*) mais la table "session"
    source vient d'une base Postgres distincte (ANALYTICS_POSTGRES_*, base
    Umami) : impossible a faire en un seul script SQL sans postgres_fdw,
    d'ou ce script Python qui reutilise les deux pools comme le cycle
    normal.
    """
    session = ClientSession()

    pool = await create_pool(
        database=environ["POSTGRES_DATABASE"],
        user=environ["POSTGRES_USER"],
        password=environ["POSTGRES_PASSWORD"],
        host=environ["POSTGRES_HOST"],
        port=environ["POSTGRES_PORT"],
        min_size=2,
        max_size=2,
    )

    analytics_pool = await create_pool(
        database=environ["ANALYTICS_POSTGRES_DATABASE"],
        user=environ["ANALYTICS_POSTGRES_USER"],
        password=environ["ANALYTICS_POSTGRES_PASSWORD"],
        host=environ["ANALYTICS_POSTGRES_HOST"],
        port=environ["ANALYTICS_POSTGRES_PORT"],
        min_size=2,
        max_size=2,
    )

    analytics = Analytics(
        session=session,
        pool=pool,
        analytics_pool=analytics_pool,
        websites_ids=environ["WEBSITES_IDS"].split(","),
        photon_api=environ.get("PHOTON_API_URL"),
    )

    await analytics.backfill()

    await session.close()
    await pool.close()
    await analytics_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
