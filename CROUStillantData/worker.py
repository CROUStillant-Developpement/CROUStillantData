from asyncpg import Pool, Connection
from json import load


class Worker:
    def __init__(
        self, pool: Pool
    ) -> None:
        """
        Constructeur de la classe Worker.

        :param pool: Le pool de connexions
        :type pool: Pool
        """
        self.pool = pool

        self.views_ref = self.__load()


    def __load(self) -> dict:
        """
        Charge le référentiel JSON.

        :return: Le référentiel JSON
        :rtype: dict
        """
        with open("/CROUStillantData/referential.json", "r", encoding="utf-8") as file:
            referential = load(file)

        return referential


    async def run(self) -> None:
        """
        Fonction principale du worker.
        """
        async with self.pool.acquire() as connection:
            connection: Connection

            for view in self.views_ref:
                print(f"Raffraîchissement de la vue matérialisée {view}...")

                await connection.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};")

        print("Raffraîchissement terminé.")
