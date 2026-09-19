from aiohttp import ClientError, ClientSession, ClientTimeout
from asyncpg import Pool
from json import dumps


class BotStats:
    """
    Relève les statistiques du bot Discord CROUStillantBot sur son endpoint
    dPyStatus (GET /status) et les enregistre dans la table bot_stats.

    Un relevé est enregistré même quand le bot est injoignable (STATUS =
    'offline', compteurs NULL) afin de pouvoir calculer l'uptime.
    """

    # Statuts renvoyés par dPyStatus (et acceptés par CK_BOT_STATS_STATUS)
    STATUSES = ("online", "degraded", "starting", "offline")

    def __init__(
        self,
        pool: Pool,
        session: ClientSession,
        url: str,
        token: str | None = None,
        timeout: float = 5,
    ) -> None:
        """
        Constructeur de la classe BotStats.

        :param pool: Le pool de connexions vers la base principale
        :type pool: Pool
        :param session: La session HTTP
        :type session: ClientSession
        :param url: L'URL complète de l'endpoint dPyStatus
        :type url: str
        :param token: Le token dPyStatus (DPYSTATUS_TOKEN du bot), si configuré
        :type token: str | None
        :param timeout: Le délai maximal de réponse du bot, en secondes
        :type timeout: float
        """
        self.pool = pool
        self.session = session
        self.url = url
        self.token = token
        self.timeout = ClientTimeout(total=timeout)

    async def fetch(self) -> dict | None:
        """
        Récupère le statut du bot.

        :return: La réponse dPyStatus, ou None si le bot est injoignable
        :rtype: dict | None
        :raises PermissionError: Si le token est refusé (erreur de configuration, pas une panne)
        """
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}

        try:
            async with self.session.get(self.url, headers=headers, timeout=self.timeout) as response:
                if response.status == 401:
                    raise PermissionError("Token dPyStatus refusé (DPYSTATUS_TOKEN)")

                # 200 (online/degraded) comme 503 (starting/offline) portent un corps valide
                return await response.json()
        except (ClientError, TimeoutError, ValueError) as error:
            print(f"Bot injoignable ({type(error).__name__}: {error})")
            return None

    async def run(self) -> None:
        """
        Fonction principale : relève le statut du bot et l'enregistre.
        """
        data = await self.fetch()

        if data is None or data.get("status") not in self.STATUSES:
            status = "offline"
            values = (None,) * 7
            payload = None
        else:
            stats = data.get("stats", {})
            status = data["status"]
            values = (
                data.get("latency_ms"),
                data.get("uptime"),
                stats.get("guilds"),
                stats.get("users"),
                stats.get("cached_users"),
                stats.get("channels", {}).get("total"),
                stats.get("shard_count"),
            )
            payload = dumps(data)

        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                    INSERT INTO bot_stats (
                        STATUS, LATENCY_MS, UPTIME_S, GUILDS, USERS, CACHED_USERS, CHANNELS, SHARDS, PAYLOAD
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb);
                """,
                status,
                *values,
                payload,
            )

        print(f"Relevé du bot enregistré : {status} ({values[2]} serveurs, {values[3]} utilisateurs)")
