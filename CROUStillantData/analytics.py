from aiohttp import ClientSession
from asyncpg import Pool, Connection
from datetime import datetime, timedelta


class Analytics:
    # Marge de securite : ne jamais compter une session dont l'ecriture
    # cote Umami pourrait etre encore en cours au moment de la requete.
    SAFETY_MARGIN = timedelta(minutes=1)

    def __init__(
        self,
        session: ClientSession,
        pool: Pool,
        analytics_pool: Pool,
        websites_ids: list[int],
        geo_api_url: str,
        geo_api_key: str | None = None,
    ) -> None:
        """
        Initialise the Analytics class.

        :param session: The aiohttp ClientSession for making HTTP requests.
        :type session: ClientSession
        :param pool: The asyncpg Pool for the main database.
        :type pool: Pool
        :param analytics_pool: The asyncpg Pool for the analytics database.
        :type analytics_pool: Pool
        :param websites_ids: List of website IDs to process.
        :type websites_ids: list[int]
        :param geo_api_url: Base URL of the GeoPol instance used for
            geocoding (e.g. https://geo.bayfield.dev).
        :type geo_api_url: str
        :param geo_api_key: GeoPol API key, sent in the X-API-Key header.
        :type geo_api_key: str | None
        """
        self.session = session
        self.pool = pool
        self.analytics_pool = analytics_pool
        self.websites_ids = websites_ids
        self.geo_search_url = f"{geo_api_url.rstrip('/')}/v1/search"
        self.geo_headers = {"X-API-Key": geo_api_key} if geo_api_key else {}

        self.df_pool = None
        self.df_analytics_pool = None

    async def __load(self) -> None:
        """
        Load data from the databases into DataFrames.
        """
        async with self.pool.acquire() as connection:
            connection: Connection

            records = await connection.fetch("SELECT * FROM GEO_DATA")
            self.df_pool = [dict(record) for record in records]

        async with self.analytics_pool.acquire() as connection:
            connection: Connection

            records = await connection.fetch(
                "SELECT * FROM session WHERE website_id = ANY($1) AND CITY IS NOT NULL;",
                self.websites_ids,
            )
            self.df_analytics_pool = [dict(record) for record in records]

        print(f"Loaded {len(self.df_pool)} records from GEO_DATA.")
        print(
            f"Loaded {len(self.df_analytics_pool)} records from analytics session table."
        )

    async def process(self) -> None:
        """
        Process the loaded data: geodecode any new French city seen in the
        analytics sessions, then increment GEO_USAGE with sessions counted
        since the last run.
        """
        await self.__load()
        known_cities = await self.__ensure_geo_data()
        await self.update_geo_usage(known_cities)

    async def backfill(self) -> None:
        """
        One-off seed of GEO_USAGE from the *entire* session history,
        ignoring geo_usage_watermark. Meant to be run manually once (see
        backfill_geo_usage.py), e.g. right after GEO_USAGE is created, or to
        recompute it from scratch — never as part of the scheduled
        process() cycle, which increments instead of overwriting.

        Sessions whose city still fails geodecoding by the end of this run
        are skipped, same as in update_geo_usage: they will only be counted
        once that city is geodecoded, and only from sessions created after
        the watermark this backfill leaves behind.
        """
        await self.__load()
        known_cities = await self.__ensure_geo_data()

        city_counts: dict[str, int] = {}
        max_created_at: datetime | None = None

        for record in self.df_analytics_pool:
            city = record.get("city")
            created_at = record.get("created_at")

            if created_at is not None and (
                max_created_at is None or created_at > max_created_at
            ):
                max_created_at = created_at

            if city and city in known_cities and record.get("country") == "FR":
                city_counts[city] = city_counts.get(city, 0) + 1

        async with self.pool.acquire() as connection:
            connection: Connection

            async with connection.transaction():
                await connection.execute("TRUNCATE GEO_USAGE")

                if city_counts:
                    await connection.executemany(
                        "INSERT INTO GEO_USAGE (CITY, TOTAL) VALUES ($1, $2);",
                        list(city_counts.items()),
                    )

                if max_created_at is not None:
                    await connection.execute(
                        "UPDATE geo_usage_watermark SET last_processed_at = $1 WHERE id = 1",
                        max_created_at,
                    )

        print(
            f"Backfill GEO_USAGE: {sum(city_counts.values())} session(s) sur {len(city_counts)} ville(s)."
        )

    async def __ensure_geo_data(self) -> set[str]:
        """
        Geodecode every distinct French city seen in the loaded analytics
        sessions that isn't already in GEO_DATA (self.df_pool).

        :return: The set of city names now known to GEO_DATA (pre-existing
            plus newly geodecoded this call).
        :rtype: set[str]
        """
        distinct_cities = {
            record.get("city")
            for record in self.df_analytics_pool
            # Limite a la France : la recherche GeoPol est filtree sur FR
            if record.get("city") and record.get("country") == "FR"
        }

        print(f"Found {len(distinct_cities)} distinct cities to geodecode.")

        known_cities = {record.get("city") for record in self.df_pool}

        for city in distinct_cities:
            if city not in known_cities:
                print(f"Geodecoding city: {city}")
                if await self.geodecode(city):
                    known_cities.add(city)

        return known_cities

    # Couches GeoPol essayees dans l'ordre : d'abord les communes, puis
    # les lieux-dits / quartiers si la ville Umami n'est pas une commune.
    # Jamais de recherche sans couche : "Paris" ne doit pas tomber sur une
    # rue de Paris ailleurs en France.
    GEO_LAYERS = ("city", "locality,district")

    async def geodecode(self, city: str) -> bool:
        """
        Geodecode a French city with GeoPol and insert it into GEO_DATA.

        :param city: The city name to geodecode.
        :type city: str
        :return: True if the city was geodecoded and inserted into GEO_DATA.
        :rtype: bool
        """
        for layers in self.GEO_LAYERS:
            try:
                async with self.session.get(
                    self.geo_search_url,
                    params={
                        "q": str(city),
                        "limit": 1,
                        "countries": "fr",
                        "layers": layers,
                        "lang": "fr",
                    },
                    headers=self.geo_headers,
                ) as response:
                    if response.status != 200:
                        print(
                            f"Error geodecoding city {city}: HTTP {response.status} {await response.text()}"
                        )
                        return False
                    data = await response.json()
            except Exception as e:
                print(f"Error geodecoding city {city}: {e}")
                return False

            if data.get("results"):
                await self.insert_geo_data(data["results"][0], city)
                return True

        return False

    async def insert_geo_data(self, result: dict, city: str) -> None:
        """
        Insert geographical data into the GEO_DATA table.

        :param result: A result from GeoPol's /v1/search response.
        :type result: dict
        :param city: The city name.
        :type city: str
        """
        address = result.get("address") or {}
        country_code = address.get("countryCode") or ""
        region = address.get("state") or ""
        latitude = result.get("lat", 0.0)
        longitude = result.get("lon", 0.0)

        print(
            f"Inserting GEO data for city: {city}, Country: {country_code}, Region: {region}, Lat: {latitude}, Lon: {longitude}"
        )

        async with self.pool.acquire() as connection:
            connection: Connection

            await connection.execute(
                """
                INSERT INTO GEO_DATA (COUNTRY_CODE, REGION, CITY, LATITUDE, LONGITUDE)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (CITY) DO NOTHING;
                """,
                country_code,
                region,
                city,
                latitude,
                longitude,
            )

    async def update_geo_usage(self, known_cities: set[str]) -> None:
        """
        Increment GEO_USAGE.TOTAL with the French sessions counted since the
        last run (geo_usage_watermark). The session table is reloaded in
        full on every run (see __load), so the watermark is what keeps a
        session from being counted twice.

        geo_usage_watermark is TIMESTAMPTZ and compared against NOW(), not
        LOCALTIMESTAMP: session.created_at comes back tz-aware from asyncpg
        (Umami, separate Postgres instance), and comparing it to a naive
        datetime raises TypeError.

        :param known_cities: Cities currently present (or just inserted) in
            GEO_DATA. A session for a city outside this set is skipped this
            round rather than violating GEO_USAGE's foreign key; it is
            counted once the city is geodecoded and falls back inside a
            later window.
        :type known_cities: set[str]
        """
        async with self.pool.acquire() as connection:
            connection: Connection

            last_processed_at: datetime = await connection.fetchval(
                "SELECT LAST_PROCESSED_AT FROM geo_usage_watermark WHERE ID = 1"
            )
            new_watermark: datetime = await connection.fetchval(
                "SELECT NOW() - $1::interval", self.SAFETY_MARGIN
            )

        if new_watermark <= last_processed_at:
            return

        city_counts: dict[str, int] = {}

        for record in self.df_analytics_pool:
            city = record.get("city")
            created_at = record.get("created_at")

            if (
                city
                and city in known_cities
                and record.get("country") == "FR"
                and created_at is not None
                and last_processed_at < created_at <= new_watermark
            ):
                city_counts[city] = city_counts.get(city, 0) + 1

        async with self.pool.acquire() as connection:
            connection: Connection

            async with connection.transaction():
                if city_counts:
                    await connection.executemany(
                        """
                        INSERT INTO GEO_USAGE (CITY, TOTAL)
                        VALUES ($1, $2)
                        ON CONFLICT (CITY) DO UPDATE SET TOTAL = GEO_USAGE.TOTAL + EXCLUDED.TOTAL;
                        """,
                        list(city_counts.items()),
                    )

                await connection.execute(
                    "UPDATE geo_usage_watermark SET last_processed_at = $1 WHERE id = 1",
                    new_watermark,
                )

        print(
            f"GEO_USAGE: {sum(city_counts.values())} session(s) comptabilisee(s) sur {len(city_counts)} ville(s)."
        )
