from asyncpg import Pool, Connection
from json import load
from datetime import datetime, timedelta


class Worker:
    # Stores the last refresh time for each view
    _last_refresh: dict[str, datetime] = {}

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


    def __load(self) -> dict[str, int]:
        """
        Charge le référentiel JSON.

        :return: Le référentiel JSON avec les vues et leurs intervalles de rafraîchissement
        :rtype: dict[str, int]
        """
        with open("/CROUStillantData/referential.json", "r", encoding="utf-8") as file:
            referential = load(file)

        return referential


    def __should_refresh(self, view: str, interval_minutes: int) -> bool:
        """
        Vérifie si une vue doit être rafraîchie en fonction de son intervalle.

        :param view: Le nom de la vue
        :type view: str
        :param interval_minutes: L'intervalle de rafraîchissement en minutes
        :type interval_minutes: int
        :return: True si la vue doit être rafraîchie, False sinon
        :rtype: bool
        """
        now = datetime.now()
        last_refresh = Worker._last_refresh.get(view)

        if last_refresh is None:
            return True

        return now - last_refresh >= timedelta(minutes=interval_minutes)


    async def run(self) -> None:
        """
        Fonction principale du worker.
        """
        async with self.pool.acquire() as connection:
            connection: Connection

            refreshed_count = 0
            skipped_count = 0

            for view, interval_minutes in self.views_ref.items():
                if not self.__should_refresh(view, interval_minutes):
                    print(f"Vue {view} non rafraîchie (intervalle: {interval_minutes} min)")
                    skipped_count += 1
                    continue

                print(f"Raffraîchissement de la vue matérialisée {view} (intervalle: {interval_minutes} min)...")

                await connection.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};")
                Worker._last_refresh[view] = datetime.now()
                refreshed_count += 1

        print(f"Raffraîchissement terminé. {refreshed_count} vue(s) rafraîchie(s), {skipped_count} vue(s) ignorée(s).")
