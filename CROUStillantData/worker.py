from asyncpg import Pool, Connection
from json import load, dump
from datetime import datetime, timedelta
from os.path import exists
import re


class Worker:
    # File path to store last refresh times
    LAST_REFRESH_FILE = "/CROUStillantData/last_refresh.json"

    # Valid view name pattern (PostgreSQL identifier)
    VIEW_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

    def __init__(
        self, pool: Pool
    ) -> None:
        """
        Constructeur de la classe Worker.

        :param pool: Le pool de connexions
        :type pool: Pool
        """
        self.pool = pool

        self.views_ref = self.__load_referential()
        self.last_refresh = self.__load_last_refresh()


    def __load_referential(self) -> dict[str, int]:
        """
        Charge le référentiel JSON.

        :return: Le référentiel JSON avec les vues et leurs intervalles de rafraîchissement
        :rtype: dict[str, int]
        """
        with open("/CROUStillantData/referential.json", "r", encoding="utf-8") as file:
            referential = load(file)

        return referential


    def __load_last_refresh(self) -> dict[str, datetime]:
        """
        Charge les derniers temps de rafraîchissement depuis le fichier JSON.

        :return: Un dictionnaire des derniers temps de rafraîchissement
        :rtype: dict[str, datetime]
        """
        if not exists(self.LAST_REFRESH_FILE):
            return {}

        try:
            with open(self.LAST_REFRESH_FILE, "r", encoding="utf-8") as file:
                data = load(file)
            return {view: datetime.fromisoformat(ts) for view, ts in data.items()}
        except (ValueError, KeyError):
            return {}


    def __save_last_refresh(self) -> None:
        """
        Sauvegarde les derniers temps de rafraîchissement dans le fichier JSON.
        """
        data = {view: ts.isoformat() for view, ts in self.last_refresh.items()}
        with open(self.LAST_REFRESH_FILE, "w", encoding="utf-8") as file:
            dump(data, file, indent=4)


    def __is_valid_view_name(self, view: str) -> bool:
        """
        Vérifie si le nom de la vue est valide (identifiant PostgreSQL).

        :param view: Le nom de la vue
        :type view: str
        :return: True si le nom est valide, False sinon
        :rtype: bool
        """
        return bool(self.VIEW_NAME_PATTERN.match(view))


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
        last_refresh = self.last_refresh.get(view)

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
                if not self.__is_valid_view_name(view):
                    print(f"Vue {view} ignorée (nom invalide)")
                    continue

                if not self.__should_refresh(view, interval_minutes):
                    print(f"Vue {view} non rafraîchie (intervalle: {interval_minutes} min)")
                    skipped_count += 1
                    continue

                print(f"Raffraîchissement de la vue matérialisée {view} (intervalle: {interval_minutes} min)...")

                await connection.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};")
                self.last_refresh[view] = datetime.now()
                refreshed_count += 1

        self.__save_last_refresh()
        print(f"Raffraîchissement terminé. {refreshed_count} vue(s) rafraîchie(s), {skipped_count} vue(s) ignorée(s).")
