import re
from uuid import UUID
from asyncpg import Pool, Connection, connect
from datetime import datetime


# Nom de partition attendu, ex: requests_logs_2025_04 (voir
# maintain_requests_logs_partitions dans CROUStillant/schema.sql)
_PARTITION_NAME_PATTERN = re.compile(r"^requests_logs_\d{4}_\d{2}$")
_BOUND_EXPR_PATTERN = re.compile(
    r"FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)"
)

# Copie server-a-server via postgres_fdw (pas de round-trip Python pour les
# colonnes JSONB) : on peut se permettre des lots plus gros que si les lignes
# transitaient par l'application. On reste toutefois mesure : chaque lot est
# une seule instruction cote serveur, et un lot trop gros allonge d'autant la
# fenetre pendant laquelle une coupure de connexion fait perdre le travail en
# cours (la copie est reprenable, mais seulement lot par lot).
_COPY_BATCH_SIZE = 5_000

_ARCHIVE_TABLE_COLUMNS = (
    "id", "key", "method", "path", "status", "params", "request_headers",
    "ratelimit_limit", "ratelimit_remaining", "ratelimit_used",
    "ratelimit_reset", "ratelimit_bucket", "process_time", "api_version",
    "hashed_ip", "created_at",
)

_ARCHIVE_TABLE_DDL_COLUMNS = """
    id UUID NOT NULL,
    key VARCHAR(50),
    method VARCHAR(500) NOT NULL,
    path TEXT NOT NULL,
    status SMALLINT NOT NULL,
    params JSONB NOT NULL,
    request_headers JSONB NOT NULL,
    ratelimit_limit INT NOT NULL,
    ratelimit_remaining INT NOT NULL,
    ratelimit_used INT NOT NULL,
    ratelimit_reset INT NOT NULL,
    ratelimit_bucket BIGINT NOT NULL,
    process_time INT NOT NULL,
    api_version VARCHAR(50) NOT NULL,
    hashed_ip VARCHAR(40),
    created_at TIMESTAMP NOT NULL
"""

_CREATE_ARCHIVE_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS requests_logs_archive(
        {_ARCHIVE_TABLE_DDL_COLUMNS},
        PRIMARY KEY (id, created_at)
    );
"""

_CREATE_ARCHIVE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_requests_logs_archive_created_at
        ON requests_logs_archive (created_at);
"""

_FOREIGN_SERVER_NAME = "requests_logs_archive_server"
_FOREIGN_TABLE_NAME = "requests_logs_archive_fdw"


def _sql_literal(value) -> str:
    """
    Échappe une valeur pour l'insérer littéralement dans du DDL (les
    OPTIONS(...) de CREATE SERVER / USER MAPPING ne peuvent pas être des
    paramètres liés — PostgreSQL ne supporte pas les bind parameters en DDL).

    :param value: Valeur à échapper
    :return: Littéral SQL entre guillemets simples, sûr à interpoler
    :rtype: str
    """
    return "'" + str(value).replace("'", "''") + "'"


class Archiver:
    """
    Copie les partitions mensuelles de requests_logs plus vieilles que
    ARCHIVE_RETENTION_MONTHS vers une base PostgreSQL distante via
    postgres_fdw, puis les detache et les supprime localement.

    La copie se fait entièrement côté serveur (INSERT INTO table_étrangère
    SELECT ... FROM partition) : les lignes ne transitent jamais par ce
    processus Python, seule la connexion FDW entre les deux PostgreSQL porte
    les données.

    Ne touche jamais aux statistiques (stats_counters, stats_hourly, ...) :
    celles-ci sont deja a jour de facon independante (voir
    stats_aggregator.py), c'est justement ce qui rend cet archivage sans
    risque pour les tableaux de bord.
    """

    def __init__(
        self,
        local_pool: Pool,
        archive_connection: dict,
        retention_months: int,
    ) -> None:
        """
        Constructeur de la classe Archiver.

        :param local_pool: Pool de connexions vers la base principale
        :type local_pool: Pool
        :param archive_connection: Paramètres de connexion à la base distante
            (host, port, database, user, password) — utilisés à la fois pour
            le bootstrap de la table distante et pour configurer le serveur
            étranger postgres_fdw côté local
        :type archive_connection: dict
        :param retention_months: Nombre de mois de donnees a garder en local
        :type retention_months: int
        """
        self.local_pool = local_pool
        self.archive_connection = archive_connection
        self.retention_months = retention_months

    async def run(self) -> None:
        """
        Fonction principale : archive puis purge localement toutes les
        partitions eligibles.
        """
        await self.__bootstrap_archive_table()

        async with self.local_pool.acquire() as local_conn:
            local_conn: Connection

            await self.__setup_fdw(local_conn)

            cutoff: datetime = await local_conn.fetchval(
                "SELECT DATE_TRUNC('month', LOCALTIMESTAMP) - MAKE_INTERVAL(months => $1::int)",
                self.retention_months,
            )

            partitions = await self.__list_eligible_partitions(local_conn, cutoff)

        if not partitions:
            print("Archiver: aucune partition éligible à l'archivage.")
            return

        # Une connexion par partition : la copie d'une grosse partition est
        # longue, et si le serveur ferme la connexion en cours de route
        # (le cas rencontre en production), on veut repartir sur une
        # connexion saine pour les partitions suivantes plutot que de faire
        # tomber toute l'execution. asyncpg jette d'office les connexions
        # cassees rendues au pool.
        for partition_name, lower_bound, upper_bound in partitions:
            try:
                async with self.local_pool.acquire() as local_conn:
                    await self.__archive_partition(
                        local_conn, partition_name, lower_bound, upper_bound
                    )
            except Exception as error:
                # La copie est reprenable : rien n'est perdu, la partition
                # reste locale et la prochaine execution repartira de la
                # derniere ligne effectivement archivee.
                print(
                    f"Archiver: échec sur {partition_name} "
                    f"({type(error).__name__}: {error}). Partition conservée "
                    f"localement, reprise à la prochaine exécution."
                )

    async def __bootstrap_archive_table(self) -> None:
        """
        Crée la table physique sur la base distante si nécessaire. Connexion
        directe et ponctuelle : une table étrangère ne peut pas créer la
        table qu'elle référence, il faut la créer côté distant d'abord.
        """
        connection = await connect(**self.archive_connection)
        try:
            await connection.execute(_CREATE_ARCHIVE_TABLE_SQL)
            await connection.execute(_CREATE_ARCHIVE_INDEX_SQL)
        finally:
            await connection.close()

    async def __setup_fdw(self, connection: Connection) -> None:
        """
        Met en place postgres_fdw côté local si nécessaire : extension,
        serveur distant, mapping utilisateur et table étrangère. Idempotent
        (vérifie l'existence avant de créer, plutôt que IF NOT EXISTS dont
        le support varie selon les objets FDW).

        :param connection: Connexion locale à utiliser
        :type connection: Connection
        """
        await connection.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;")

        server_exists = await connection.fetchval(
            "SELECT 1 FROM pg_foreign_server WHERE srvname = $1", _FOREIGN_SERVER_NAME
        )
        if not server_exists:
            await connection.execute(
                f"""
                CREATE SERVER {_FOREIGN_SERVER_NAME}
                FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (
                    host {_sql_literal(self.archive_connection["host"])},
                    port {_sql_literal(self.archive_connection["port"])},
                    dbname {_sql_literal(self.archive_connection["database"])}
                );
                """
            )

        mapping_exists = await connection.fetchval(
            """
            SELECT 1 FROM pg_user_mappings
            WHERE srvname = $1 AND usename = current_user
            """,
            _FOREIGN_SERVER_NAME,
        )
        if not mapping_exists:
            await connection.execute(
                f"""
                CREATE USER MAPPING FOR CURRENT_USER SERVER {_FOREIGN_SERVER_NAME}
                OPTIONS (
                    user {_sql_literal(self.archive_connection["user"])},
                    password {_sql_literal(self.archive_connection["password"])}
                );
                """
            )

        foreign_table_exists = await connection.fetchval(
            """
            SELECT 1 FROM pg_foreign_table ft
            JOIN pg_class c ON c.oid = ft.ftrelid
            WHERE c.relname = $1
            """,
            _FOREIGN_TABLE_NAME,
        )
        if not foreign_table_exists:
            await connection.execute(
                f"""
                CREATE FOREIGN TABLE {_FOREIGN_TABLE_NAME} (
                    {_ARCHIVE_TABLE_DDL_COLUMNS}
                )
                SERVER {_FOREIGN_SERVER_NAME}
                OPTIONS (table_name 'requests_logs_archive');
                """
            )

    async def __list_eligible_partitions(
        self, connection: Connection, cutoff: datetime
    ) -> list[tuple[str, datetime, datetime]]:
        """
        Liste les partitions de requests_logs entièrement antérieures à
        `cutoff` et pas encore archivées.

        :param connection: Connexion locale à utiliser
        :type connection: Connection
        :param cutoff: Date limite (les partitions strictement avant ne sont pas archivées)
        :type cutoff: datetime
        :return: Liste de tuples (nom_partition, borne_inferieure, borne_superieure),
            triée par date croissante
        :rtype: list[tuple[str, datetime, datetime]]
        """
        rows = await connection.fetch(
            """
            SELECT
                child.relname AS partition_name,
                pg_get_expr(child.relpartbound, child.oid) AS bound_expr
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid = child.oid
            WHERE parent.relname = 'requests_logs'
            ORDER BY child.relname;
            """
        )

        already_archived = {
            r["partition_name"]
            for r in await connection.fetch("SELECT partition_name FROM archive_log")
        }

        eligible: list[tuple[str, datetime, datetime]] = []
        for row in rows:
            name = row["partition_name"]
            if name in already_archived or not _PARTITION_NAME_PATTERN.match(name):
                continue

            match = _BOUND_EXPR_PATTERN.search(row["bound_expr"] or "")
            if not match:
                continue

            lower_bound = datetime.fromisoformat(match.group(1))
            upper_bound = datetime.fromisoformat(match.group(2))
            if upper_bound <= cutoff:
                eligible.append((name, lower_bound, upper_bound))

        eligible.sort(key=lambda item: item[2])
        return eligible

    async def __archive_partition(
        self,
        local_conn: Connection,
        partition_name: str,
        lower_bound: datetime,
        upper_bound: datetime,
    ) -> None:
        """
        Archive puis supprime une partition : verifie que les statistiques
        l'ont deja integree, copie ses lignes vers la base distante par
        lots via postgres_fdw, verifie le compte, puis detache et supprime
        la table locale.

        :param local_conn: Connexion locale à utiliser
        :type local_conn: Connection
        :param partition_name: Nom de la partition (deja valide contre _PARTITION_NAME_PATTERN)
        :type partition_name: str
        :param lower_bound: Borne inferieure (inclusive) de la partition
        :type lower_bound: datetime
        :param upper_bound: Borne superieure (exclusive) de la partition
        :type upper_bound: datetime
        """
        if not await self.__stats_are_ready(
            local_conn, partition_name, lower_bound, upper_bound
        ):
            return

        local_count: int = await local_conn.fetchval(
            f"SELECT COUNT(*) FROM {partition_name}"
        )

        archived = await self.__copy_partition(
            local_conn, partition_name, lower_bound, upper_bound
        )

        if archived != local_count:
            print(
                f"Archiver: ABANDON de {partition_name} — {archived} lignes présentes "
                f"côté archive pour {local_count} attendues. Partition conservée localement."
            )
            return

        async with local_conn.transaction():
            await local_conn.execute(
                f"ALTER TABLE requests_logs DETACH PARTITION {partition_name};"
            )
            await local_conn.execute(f"DROP TABLE {partition_name};")
            await local_conn.execute(
                "INSERT INTO archive_log (partition_name, row_count) VALUES ($1, $2)",
                partition_name,
                archived,
            )

        print(f"Archiver: {partition_name} archivée ({archived} lignes) et supprimée localement.")

    async def __stats_are_ready(
        self,
        local_conn: Connection,
        partition_name: str,
        lower_bound: datetime,
        upper_bound: datetime,
    ) -> bool:
        """
        Verifie que StatsAggregator a bien fige toutes les statistiques
        derivees de cette partition avant qu'on la supprime.

        Le watermark ne suffit pas : il ne couvre que les compteurs
        cumulatifs (stats_counters, stats_by_*, ...), qui avancent a chaque
        execution. Les rollups stats_hourly / stats_daily, eux, sont
        calcules depuis requests_logs tranche par tranche, avec un
        rattrapage borne par execution (_MAX_CATCHUP_HOURS /
        _MAX_CATCHUP_DAYS) : supprimer une partition avant qu'ils l'aient
        finalisee laisserait des trous definitifs dans v_gf_hourly_* et
        v_gf_daily_status_*, impossibles a combler une fois les lignes
        parties (StatsAggregator refuse, a raison, d'inventer des zeros pour
        une periode qui n'est plus dans requests_logs).

        On compte donc explicitement les tranches manquantes sur l'intervalle
        de la partition, plutot que de comparer a MAX(hour) / MAX(day) : ces
        maximums ne garantissent rien sur les trous situes en dessous d'eux.

        :param local_conn: Connexion locale à utiliser
        :type local_conn: Connection
        :param partition_name: Nom de la partition concernee
        :type partition_name: str
        :param lower_bound: Borne inferieure (inclusive) de la partition
        :type lower_bound: datetime
        :param upper_bound: Borne superieure (exclusive) de la partition
        :type upper_bound: datetime
        :return: True si toutes les statistiques couvrent deja la partition
        :rtype: bool
        """
        watermark: datetime = await local_conn.fetchval(
            "SELECT last_processed_at FROM stats_watermark WHERE id = 1"
        )
        if watermark < upper_bound:
            print(
                f"Archiver: {partition_name} ignorée — StatsAggregator n'a pas "
                f"encore traité toutes ses lignes (watermark={watermark})."
            )
            return False

        gaps = await local_conn.fetchrow(
            """
            SELECT
                (
                    SELECT COUNT(*)
                    FROM generate_series(
                        $1::timestamp,
                        $2::timestamp - INTERVAL '1 hour',
                        INTERVAL '1 hour'
                    ) AS h
                    WHERE NOT EXISTS (
                        SELECT 1 FROM stats_hourly s WHERE s.hour = h
                    )
                ) AS missing_hours,
                (
                    SELECT COUNT(*)
                    FROM generate_series(
                        $1::timestamp,
                        $2::timestamp - INTERVAL '1 day',
                        INTERVAL '1 day'
                    ) AS d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM stats_daily s WHERE s.day = d::date
                    )
                ) AS missing_days
            """,
            lower_bound,
            upper_bound,
        )

        if gaps["missing_hours"] or gaps["missing_days"]:
            print(
                f"Archiver: {partition_name} ignorée — rollups incomplets sur "
                f"cette période ({gaps['missing_hours']} heure(s) et "
                f"{gaps['missing_days']} jour(s) manquants). StatsAggregator "
                f"les comblera lors de ses prochains passages."
            )
            return False

        return True

    async def __remote_count(
        self, local_conn: Connection, lower_bound: datetime, upper_bound: datetime
    ) -> int:
        """
        Compte les lignes deja presentes cote archive sur l'intervalle d'une
        partition. Le COUNT(*) est pousse tel quel au serveur distant par
        postgres_fdw : aucune ligne ne transite.

        :param local_conn: Connexion locale à utiliser
        :type local_conn: Connection
        :param lower_bound: Borne inferieure (inclusive) de la partition
        :type lower_bound: datetime
        :param upper_bound: Borne superieure (exclusive) de la partition
        :type upper_bound: datetime
        :return: Nombre de lignes archivees sur cet intervalle
        :rtype: int
        """
        return await local_conn.fetchval(
            f"""
            SELECT COUNT(*) FROM {_FOREIGN_TABLE_NAME}
            WHERE created_at >= $1 AND created_at < $2
            """,
            lower_bound,
            upper_bound,
        )

    async def __copy_partition(
        self,
        local_conn: Connection,
        partition_name: str,
        lower_bound: datetime,
        upper_bound: datetime,
    ) -> int:
        """
        Copie toutes les lignes d'une partition vers la table étrangère (donc
        vers la base distante), par lots, entièrement côté serveur : INSERT
        INTO table_étrangère SELECT ... FROM partition. Seules les colonnes
        (created_at, id) de chaque lot reviennent au client, pour paginer par
        clé — les colonnes lourdes (params, request_headers, ...) ne quittent
        jamais PostgreSQL.

        L'opération est reprenable : une exécution interrompue (perte de la
        connexion pendant la copie) laisse des lignes déjà écrites côté
        archive. On repart donc du dernier created_at déjà archivé sur
        l'intervalle de la partition, et chaque lot est inséré en ON CONFLICT
        DO NOTHING pour absorber le recouvrement au point de reprise
        (plusieurs lignes peuvent partager le même created_at) — sans quoi la
        reprise échoue sur requests_logs_archive_pkey.

        :param local_conn: Connexion locale à utiliser
        :type local_conn: Connection
        :param partition_name: Nom de la partition source
        :type partition_name: str
        :param lower_bound: Borne inferieure (inclusive) de la partition
        :type lower_bound: datetime
        :param upper_bound: Borne superieure (exclusive) de la partition
        :type upper_bound: datetime
        :return: Nombre de lignes presentes cote archive pour cette partition
        :rtype: int
        """
        columns = ", ".join(_ARCHIVE_TABLE_COLUMNS)
        copy_batch_sql = f"""
            WITH batch AS MATERIALIZED (
                SELECT {columns}
                FROM {partition_name}
                WHERE (created_at, id) > ($1, $2) AND created_at < $3
                ORDER BY created_at, id
                LIMIT {_COPY_BATCH_SIZE}
            ), copied AS (
                INSERT INTO {_FOREIGN_TABLE_NAME} ({columns})
                SELECT {columns} FROM batch
                ON CONFLICT DO NOTHING
            )
            SELECT created_at, id FROM batch ORDER BY created_at, id
        """

        # Reprise : on redemarre AU dernier instant deja archive (et non
        # apres lui) pour ne pas sauter les lignes qui partagent ce
        # created_at et n'auraient pas ete inserees avant la coupure.
        resume_at: datetime | None = await local_conn.fetchval(
            f"""
            SELECT MAX(created_at) FROM {_FOREIGN_TABLE_NAME}
            WHERE created_at >= $1 AND created_at < $2
            """,
            lower_bound,
            upper_bound,
        )
        if resume_at is not None:
            print(
                f"Archiver: reprise de {partition_name} à partir de {resume_at} "
                f"(lignes déjà archivées par une exécution précédente)."
            )

        last_created_at = resume_at if resume_at is not None else datetime.min
        last_id = UUID(int=0)

        while True:
            rows = await local_conn.fetch(
                copy_batch_sql,
                last_created_at,
                last_id,
                upper_bound,
            )
            if not rows:
                break

            last_row = rows[-1]
            last_created_at = last_row["created_at"]
            last_id = last_row["id"]

            if len(rows) < _COPY_BATCH_SIZE:
                break

        return await self.__remote_count(local_conn, lower_bound, upper_bound)
