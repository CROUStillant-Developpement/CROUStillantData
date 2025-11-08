from aiohttp import ClientSession
from asyncpg import Pool, Connection


class Analytics:
    def __init__(self, session: ClientSession, pool: Pool, analytics_pool: Pool, websites_ids: list[int], photon_api: str) -> None:
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

            records = await connection.fetch("SELECT * FROM session WHERE website_id = ANY($1) AND CITY IS NOT NULL;", self.websites_ids)
            self.df_analytics_pool = [dict(record) for record in records]

        print(f"Loaded {len(self.df_pool)} records from GEO_DATA.")
        print(f"Loaded {len(self.df_analytics_pool)} records from analytics session table.")

    async def process(self) -> None:
        """
        Process the loaded data.
        """
        await self.__load()

        distinct_cities = set()

        for record in self.df_analytics_pool:
            city = record.get("city")
            # Limiting to France for now
            # Our Geodecode API is only configured for France for the moment
            if city and city != "" and record.get("country") == "FR":
                distinct_cities.add(city)

        print(f"Found {len(distinct_cities)} distinct cities to geodecode.")

        distinct_cities_with_user_count = []
        for city in distinct_cities:
            user_count = sum(1 for record in self.df_analytics_pool if record.get("city") == city)
            distinct_cities_with_user_count.append((city, user_count))

        for city, users in distinct_cities_with_user_count:
            if not any(geo_record for geo_record in self.df_pool if geo_record.get("CITY") == city):
                print(f"Geodecoding city: {city} ({users} users)")
                await self.geodecode(city, users)

    async def geodecode(self, city: str, users: int) -> None:
        """
        Geodecode a city to get its geographical information.

        :param city: The city name to geodecode.
        :type city: str
        :param users: The number of users from this city.
        :type users: int
        """
        try:
            async with self.session.get(
                self.photon_api_url,
                params={"q": str(city), "limit": 1}
            ) as response:
                data = await response.json()
        except Exception as e:
            print(f"Error geodecoding city {city}: {e}")
        else:
            if data["features"]:
                await self.insert_geo_data(data["features"][0], city, users)

    async def insert_geo_data(self, feature: dict, city: str, users: int) -> None:
        """
        Insert geographical data into the GEO_DATA table.
        
        :param feature: The feature dictionary from the photon API response.
        :type feature: dict
        :param city: The city name.
        :type city: str
        :param users: The number of users from this city.
        :type users: int
        """
        properties = feature.get("properties", {})
        country_code = properties.get("countrycode", "")
        region = properties.get("state", "")
        coordinates = feature.get("geometry", {}).get("coordinates", [0.0, 0.0])
        longitude = coordinates[0]
        latitude = coordinates[1]

        print(f"Inserting GEO data for city: {city}, Country: {country_code}, Region: {region}, Lat: {latitude}, Lon: {longitude}")

        async with self.pool.acquire() as connection:
            connection: Connection

            await connection.execute(
                """
                INSERT INTO GEO_DATA (COUNTRY_CODE, REGION, CITY, LATITUDE, LONGITUDE, USER_COUNT)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (CITY) DO NOTHING;
                """,
                country_code,
                region,
                city,
                latitude,
                longitude,
                users
            )
