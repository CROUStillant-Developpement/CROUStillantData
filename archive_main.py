import asyncio

from CROUStillantData.archiver import Archiver
from asyncpg import create_pool
from os import environ
from dotenv import load_dotenv


load_dotenv(dotenv_path="/CROUStillantData/.env")


async def main():
    """
    Point d'entrée de l'archivage quotidien des vieilles partitions de
    requests_logs (voir crontab). Séparé de __main__.py : contrairement au
    cycle de rafraîchissement des statistiques (toutes les 5-30 min), cette
    opération est lourde (copie réseau + DROP TABLE) et n'a besoin de
    tourner qu'une fois par jour.
    """
    local_pool = await create_pool(
        database=environ["POSTGRES_DATABASE"],
        user=environ["POSTGRES_USER"],
        password=environ["POSTGRES_PASSWORD"],
        host=environ["POSTGRES_HOST"],
        port=environ["POSTGRES_PORT"],
        min_size=2,
        max_size=2,
    )

    # Pas de pool ici : Archiver ouvre une connexion directe ponctuelle pour
    # le bootstrap de la table distante, puis passe par postgres_fdw (mis en
    # place côté local) pour toute la copie — voir archiver.py.
    archive_connection = {
        "database": environ["ARCHIVE_POSTGRES_DATABASE"],
        "user": environ["ARCHIVE_POSTGRES_USER"],
        "password": environ["ARCHIVE_POSTGRES_PASSWORD"],
        "host": environ["ARCHIVE_POSTGRES_HOST"],
        "port": environ["ARCHIVE_POSTGRES_PORT"],
    }

    archiver = Archiver(
        local_pool=local_pool,
        archive_connection=archive_connection,
        retention_months=int(environ.get("ARCHIVE_RETENTION_MONTHS", 3)),
    )
    await archiver.run()

    await local_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
