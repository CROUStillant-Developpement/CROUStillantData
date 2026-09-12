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
        photon_api: str,
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
        :param photon_api: The Photon API URL for geocoding.
        :type photon_api: str
        """
        self.session = session
        self.pool = pool
        self.analytics_pool = analytics_pool
        self.websites_ids = websites_ids
        self.photon_api_url = photon_api

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

        distinct_cities = {
            record.get("city")
            for record in self.df_analytics_pool
            # Limiting to France for now
            # Our Geodecode API is only configured for France for the moment
            if record.get("city") and record.get("country") == "FR"
        }

        print(f"Found {len(distinct_cities)} distinct cities to geodecode.")

        known_cities = {record.get("city") for record in self.df_pool}

        for city in distinct_cities:
            if city not in known_cities:
                print(f"Geodecoding city: {city}")
                if await self.geodecode(city):
                    known_cities.add(city)

        await self.update_geo_usage(known_cities)

    async def geodecode(self, city: str) -> bool:
        """
        Geodecode a city to get its geographical information.

        :param city: The city name to geodecode.
        :type city: str
        :return: True if the city was geodecoded and inserted into GEO_DATA.
        :rtype: bool
        """
        try:
            async with self.session.get(
                self.photon_api_url, params={"q": str(city), "limit": 1}
            ) as response:
                data = await response.json()
        except Exception as e:
            print(f"Error geodecoding city {city}: {e}")
            return False

        if not data["features"]:
            return False

        await self.insert_geo_data(data["features"][0], city)
        return True

    async def insert_geo_data(self, feature: dict, city: str) -> None:
        """
        Insert geographical data into the GEO_DATA table.

        :param feature: The feature dictionary from the photon API response.
        :type feature: dict
        :param city: The city name.
        :type city: str
        """
        properties = feature.get("properties", {})
        country_code = properties.get("countrycode", "")
        region = properties.get("state", "")
        coordinates = feature.get("geometry", {}).get("coordinates", [0.0, 0.0])
        longitude = coordinates[0]
        latitude = coordinates[1]

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
                "SELECT LOCALTIMESTAMP - $1::interval", self.SAFETY_MARGIN
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
