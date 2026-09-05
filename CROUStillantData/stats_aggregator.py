from asyncpg import Pool, Connection
from datetime import datetime, timedelta


# Nombre maximal de tranches (heures/jours) rattrapees en une seule execution.
# Protection contre un rattrapage demesure si le worker est reste hors ligne
# longtemps ; au-dela, il faut relancer backfill_stats.sql.
_MAX_CATCHUP_HOURS = 48
_MAX_CATCHUP_DAYS = 31


class StatsAggregator:
    """
    Maintient de façon incrémentale les tables de statistiques permanentes
    (stats_counters, stats_by_method, stats_by_api_version, stats_by_param,
    unique_hashed_ips, unique_keys, stats_hour_of_day_24h,
    unique_ips_by_hour_of_day, stats_hourly, stats_daily) à partir de
    requests_logs, sans jamais rescanner la table entière.

    Chaque exécution ne lit que la fenêtre de lignes ajoutées depuis le
    dernier passage (stats_watermark), puis "finalise" les heures/jours
    calendaires désormais clos. Ainsi requests_logs peut être purgée
    (archivage des vieilles partitions, voir archiver.py) sans faire bouger
    ces statistiques : elles ne dépendent plus des lignes déjà traitées.
    """

    # Marge de sécurité pour ne jamais traiter une ligne encore en cours
    # d'écriture par le flush périodique de CROUStillantAPI (batch toutes les
    # 10s / 500 lignes, voir components/analytics.py)
    SAFETY_MARGIN = timedelta(minutes=1)

    def __init__(self, pool: Pool) -> None:
        """
        Constructeur de la classe StatsAggregator.

        :param pool: Le pool de connexions vers la base principale
        :type pool: Pool
        """
        self.pool = pool

    async def run(self) -> None:
        """
        Fonction principale : traite la fenêtre de lignes non encore vues,
        puis finalise les heures/jours calendaires clos.
        """
        async with self.pool.acquire() as connection:
            connection: Connection
            await self.__process_window(connection)
            await self.__finalize_hours(connection)
            await self.__finalize_days(connection)

    async def __process_window(self, connection: Connection) -> None:
        """
        Traite en une transaction toutes les lignes ajoutées depuis le
        dernier watermark et avance ce dernier.

        :param connection: Connexion à utiliser
        :type connection: Connection
        """
        last_processed_at: datetime = await connection.fetchval(
            "SELECT LAST_PROCESSED_AT FROM stats_watermark WHERE ID = 1"
        )
        new_watermark: datetime = await connection.fetchval(
            "SELECT NOW() - $1::interval", self.SAFETY_MARGIN
        )

        if new_watermark <= last_processed_at:
            return

        async with connection.transaction():
            await connection.execute(
                """
                WITH window AS (
                    SELECT *
                    FROM requests_logs
                    WHERE created_at > $1 AND created_at <= $2
                ),
                agg AS (
                    SELECT
                        COUNT(*) AS total_requests,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND path = '/v1/status') AS status_requests,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 200) AS status_200,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 404) AS status_404,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 500) AS status_500,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 503) AS status_503,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 302) AS breakdown_302,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 400) AS breakdown_400,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 405) AS breakdown_405,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND status = 429) AS breakdown_429,
                        COUNT(*) FILTER (WHERE ratelimit_limit >= 0 AND key IS NOT NULL) AS requests_with_key,
                        COALESCE(MAX(ratelimit_used), 0) AS max_ratelimit_used,
                        COALESCE(SUM(ratelimit_used) FILTER (WHERE ratelimit_used >= 0), 0) AS sum_ratelimit_used,
                        COUNT(*) FILTER (WHERE ratelimit_used >= 0) AS count_ratelimit_used,
                        COALESCE(MAX(ratelimit_limit) FILTER (WHERE ratelimit_limit >= 0), 0) AS max_ratelimit_limit
                    FROM window
                )
                UPDATE stats_counters SET
                    total_requests = stats_counters.total_requests + agg.total_requests,
                    status_requests = stats_counters.status_requests + agg.status_requests,
                    status_200 = stats_counters.status_200 + agg.status_200,
                    status_404 = stats_counters.status_404 + agg.status_404,
                    status_500 = stats_counters.status_500 + agg.status_500,
                    status_503 = stats_counters.status_503 + agg.status_503,
                    breakdown_302 = stats_counters.breakdown_302 + agg.breakdown_302,
                    breakdown_400 = stats_counters.breakdown_400 + agg.breakdown_400,
                    breakdown_405 = stats_counters.breakdown_405 + agg.breakdown_405,
                    breakdown_429 = stats_counters.breakdown_429 + agg.breakdown_429,
                    requests_with_key = stats_counters.requests_with_key + agg.requests_with_key,
                    max_ratelimit_used = GREATEST(stats_counters.max_ratelimit_used, agg.max_ratelimit_used),
                    sum_ratelimit_used = stats_counters.sum_ratelimit_used + agg.sum_ratelimit_used,
                    count_ratelimit_used = stats_counters.count_ratelimit_used + agg.count_ratelimit_used,
                    max_ratelimit_limit = GREATEST(stats_counters.max_ratelimit_limit, agg.max_ratelimit_limit)
                FROM agg
                WHERE stats_counters.id = 1;
                """,
                last_processed_at,
                new_watermark,
            )

            await connection.execute(
                """
                INSERT INTO stats_by_method (method, total)
                SELECT method, COUNT(*)
                FROM requests_logs
                WHERE created_at > $1 AND created_at <= $2 AND ratelimit_limit >= 0
                GROUP BY method
                ON CONFLICT (method) DO UPDATE SET total = stats_by_method.total + EXCLUDED.total;
                """,
                last_processed_at,
                new_watermark,
            )

            await connection.execute(
                """
                INSERT INTO stats_by_api_version (version, total)
                SELECT api_version, COUNT(*)
                FROM requests_logs
                WHERE created_at > $1 AND created_at <= $2 AND ratelimit_limit >= 0
                GROUP BY api_version
                ON CONFLICT (version) DO UPDATE SET total = stats_by_api_version.total + EXCLUDED.total;
                """,
                last_processed_at,
                new_watermark,
            )

            await connection.execute(
                """
                INSERT INTO stats_by_param (param, total)
                SELECT p.key, COUNT(*)
                FROM requests_logs, jsonb_each_text(params) AS p
                WHERE created_at > $1 AND created_at <= $2
                GROUP BY p.key
                ON CONFLICT (param) DO UPDATE SET total = stats_by_param.total + EXCLUDED.total;
                """,
                last_processed_at,
                new_watermark,
            )

            await connection.execute(
                """
                INSERT INTO stats_hour_of_day_24h (hour_of_day, total)
                SELECT EXTRACT(HOUR FROM created_at)::smallint, COUNT(*)
                FROM requests_logs
                WHERE created_at > $1 AND created_at <= $2 AND ratelimit_limit >= 0
                GROUP BY 1
                ON CONFLICT (hour_of_day) DO UPDATE SET total = stats_hour_of_day_24h.total + EXCLUDED.total;
                """,
                last_processed_at,
                new_watermark,
            )

            await connection.execute(
                """
                INSERT INTO unique_hashed_ips (hashed_ip)
                SELECT DISTINCT hashed_ip
                FROM requests_logs
                WHERE created_at > $1 AND created_at <= $2 AND hashed_ip IS NOT NULL
                ON CONFLICT DO NOTHING;
                """,
                last_processed_at,
                new_watermark,
            )

            await connection.execute(
                """
                INSERT INTO unique_keys (key)
                SELECT DISTINCT key
                FROM requests_logs
                WHERE created_at > $1 AND created_at <= $2 AND ratelimit_limit >= 0 AND key IS NOT NULL
                ON CONFLICT DO NOTHING;
                """,
                last_processed_at,
                new_watermark,
            )

            await connection.execute(
                """
                INSERT INTO unique_ips_by_hour_of_day (hour_of_day, hashed_ip)
                SELECT DISTINCT EXTRACT(HOUR FROM created_at)::smallint, hashed_ip
                FROM requests_logs
                WHERE created_at > $1 AND created_at <= $2 AND ratelimit_limit >= 0 AND hashed_ip IS NOT NULL
                ON CONFLICT DO NOTHING;
                """,
                last_processed_at,
                new_watermark,
            )

            await connection.execute(
                "UPDATE stats_watermark SET last_processed_at = $1 WHERE id = 1",
                new_watermark,
            )

        print(f"StatsAggregator: fenêtre {last_processed_at} -> {new_watermark} traitée.")

    async def __finalize_hours(self, connection: Connection) -> None:
        """
        Calcule et insère les lignes stats_hourly manquantes pour toutes les
        heures calendaires désormais entièrement closes (bornées à
        _MAX_CATCHUP_HOURS par exécution).

        :param connection: Connexion à utiliser
        :type connection: Connection
        """
        last_closed_hour: datetime = await connection.fetchval(
            "SELECT DATE_TRUNC('hour', NOW()) - INTERVAL '1 hour'"
        )
        last_present_hour: datetime | None = await connection.fetchval(
            "SELECT MAX(hour) FROM stats_hourly"
        )

        if last_present_hour is None:
            # Aucune donnée : rien à rattraper ici, voir backfill_stats.sql
            return

        hour = last_present_hour + timedelta(hours=1)
        finalized = 0
        while hour <= last_closed_hour and finalized < _MAX_CATCHUP_HOURS:
            await connection.execute(
                """
                INSERT INTO stats_hourly (
                    hour, requests, error_count, unique_visitors,
                    sum_ratelimit_used, count_ratelimit_used,
                    sum_process_time, count_process_time, under_200ms_count
                )
                SELECT
                    $1,
                    COUNT(*),
                    COUNT(*) FILTER (WHERE status >= 400),
                    COUNT(DISTINCT hashed_ip),
                    COALESCE(SUM(ratelimit_used) FILTER (WHERE ratelimit_limit >= 0), 0),
                    COUNT(*) FILTER (WHERE ratelimit_limit >= 0),
                    COALESCE(SUM(process_time) FILTER (WHERE ratelimit_limit >= 0), 0),
                    COUNT(*) FILTER (WHERE ratelimit_limit >= 0),
                    COUNT(*) FILTER (WHERE process_time < 200)
                FROM requests_logs
                WHERE created_at >= $1 AND created_at < $1 + INTERVAL '1 hour'
                ON CONFLICT (hour) DO NOTHING;
                """,
                hour,
            )
            hour += timedelta(hours=1)
            finalized += 1

        if finalized:
            print(f"StatsAggregator: {finalized} heure(s) finalisée(s) dans stats_hourly.")

    async def __finalize_days(self, connection: Connection) -> None:
        """
        Calcule et insère les lignes stats_daily manquantes pour tous les
        jours calendaires désormais entièrement clos (bornées à
        _MAX_CATCHUP_DAYS par exécution).

        :param connection: Connexion à utiliser
        :type connection: Connection
        """
        last_closed_day = await connection.fetchval("SELECT CURRENT_DATE - 1")
        last_present_day = await connection.fetchval("SELECT MAX(day) FROM stats_daily")

        if last_present_day is None:
            return

        day = last_present_day + timedelta(days=1)
        finalized = 0
        while day <= last_closed_day and finalized < _MAX_CATCHUP_DAYS:
            await connection.execute(
                """
                INSERT INTO stats_daily (
                    day, status_200, status_302, status_400, status_404,
                    status_405, status_429, status_500, status_503
                )
                SELECT
                    $1,
                    COUNT(*) FILTER (WHERE status = 200),
                    COUNT(*) FILTER (WHERE status = 302),
                    COUNT(*) FILTER (WHERE status = 400),
                    COUNT(*) FILTER (WHERE status = 404),
                    COUNT(*) FILTER (WHERE status = 405),
                    COUNT(*) FILTER (WHERE status = 429),
                    COUNT(*) FILTER (WHERE status = 500),
                    COUNT(*) FILTER (WHERE status = 503)
                FROM requests_logs
                WHERE created_at >= $1 AND created_at < $1 + INTERVAL '1 day' AND ratelimit_limit >= 0
                ON CONFLICT (day) DO NOTHING;
                """,
                day,
            )
            day += timedelta(days=1)
            finalized += 1

        if finalized:
            print(f"StatsAggregator: {finalized} jour(s) finalisé(s) dans stats_daily.")
