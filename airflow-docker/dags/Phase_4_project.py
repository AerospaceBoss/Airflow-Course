from airflow.sdk import dag, task

import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import psycopg2


# ============================================================
# Configuration
# ============================================================

coords = [
    {
        "name": "cairo",
        "latitude": 30.0444,
        "longitude": 31.2357
    },
    {
        "name": "London",
        "latitude": 51.5074,
        "longitude": -0.1278
    },
    {
        "name": "New York",
        "latitude": 40.7128,
        "longitude": -74.0060
    }
]

cairo_zone = ZoneInfo("Africa/Cairo")


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="phase_4_project",
    start_date=datetime(
        2022,
        8,
        15,
        20,
        30,
        tzinfo=cairo_zone
    ),
    schedule=timedelta(days=3),
    catchup=False,
    is_paused_upon_creation=True
)
def DAG():

    # ========================================================
    # EXTRACT
    # ========================================================

    @task
    def Extract(**kwargs):

        result = []

        # Airflow gives us the start and end of this DAG run
        data_interval_start = kwargs["data_interval_start"]
        data_interval_end = kwargs["data_interval_end"]

        # Convert dates to strings
        start_date = data_interval_start.strftime("%Y-%m-%d")

        # Open-Meteo end_date is inclusive.
        # Therefore, subtract one day from Airflow's interval end.
        end_date = (
            data_interval_end - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        print(f"Extracting data from {start_date} to {end_date}")

        for city in coords:

            city_name = city["name"]
            latitude = city["latitude"]
            longitude = city["longitude"]

            # Open-Meteo API URL
            url = (
                "https://archive-api.open-meteo.com/v1/era5"
                f"?latitude={latitude}"
                f"&longitude={longitude}"
                f"&start_date={start_date}"
                f"&end_date={end_date}"
                "&hourly="
                "temperature_2m,"
                "relative_humidity_2m,"
                "wind_speed_10m"
            )

            print(f"Requesting weather data for {city_name}")

            response = requests.get(
                url,
                timeout=30
            )

            # Fail the task if API returns an HTTP error
            response.raise_for_status()

            data = response.json()

            print(
                f"STATUS for {city_name}: "
                f"{response.status_code}"
            )

            # Extract hourly data
            time_list = data["hourly"]["time"]
            wind_speed_list = data["hourly"]["wind_speed_10m"]
            humidity_list = data["hourly"]["relative_humidity_2m"]
            temperature_list = data["hourly"]["temperature_2m"]

            result.append(
                {
                    "name": city_name,
                    "time": time_list,
                    "wind": wind_speed_list,
                    "humidity": humidity_list,
                    "temperature": temperature_list
                }
            )

        return result


    # ========================================================
    # TRANSFORM
    # ========================================================

    @task
    def Transform(data: list):

        result = []

        for city in data:

            city_name = city["name"]

            time_list = city["time"]
            wind_speed_list = city["wind"]
            humidity_list = city["humidity"]
            temperature_list = city["temperature"]

            # Create DataFrame
            df = pd.DataFrame(
                {
                    "city_name": city_name,
                    "time": time_list,
                    "wind": wind_speed_list,
                    "relative_humidity": humidity_list,
                    "temperature": temperature_list
                }
            )

            # ------------------------------------------------
            # Handle NaN values
            # ------------------------------------------------

            df = df.dropna()

            # ------------------------------------------------
            # Type handling
            # ------------------------------------------------

            df["time"] = pd.to_datetime(df["time"])

            df["temperature"] = (
                df["temperature"]
                .astype(int)
            )

            df["relative_humidity"] = (
                df["relative_humidity"]
                .astype(int)
            )

            df["wind"] = (
                df["wind"]
                .astype(int)
            )

            # ------------------------------------------------
            # Convert DataFrame to normal Python dictionaries
            #
            # This is better for Airflow XCom than passing
            # pandas DataFrames directly.
            # ------------------------------------------------

            records = df.to_dict(orient="records")

            # Convert pandas Timestamp to string
            for record in records:
                record["time"] = record["time"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

            result.extend(records)

        print(f"Total transformed records: {len(result)}")

        return result


    # ========================================================
    # LOAD
    # ========================================================

    @task
    def Load(data: list):

        # ----------------------------------------------------
        # PostgreSQL connection information
        # ----------------------------------------------------

        PG_HOST = "postgres"
        PG_PORT = 5432
        PG_USER = "airflow"
        PG_PASSWORD = "airflow"

        PG_ADMIN_DB = "airflow"
        PG_TARGET_DB = "weatherdb"

        # ====================================================
        # 1. Connect to the existing Airflow database
        # ====================================================

        admin_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_ADMIN_DB
        )

        # CREATE DATABASE cannot run inside a transaction
        admin_conn.autocommit = True

        admin_cursor = admin_conn.cursor()

        # Check whether weatherdb already exists
        admin_cursor.execute(
            """
            SELECT 1
            FROM pg_database
            WHERE datname = %s
            """,
            (PG_TARGET_DB,)
        )

        database_exists = admin_cursor.fetchone()

        if not database_exists:

            print(f"Creating database: {PG_TARGET_DB}")

            admin_cursor.execute(
                f"CREATE DATABASE {PG_TARGET_DB}"
            )

        else:

            print(
                f"Database {PG_TARGET_DB} "
                "already exists"
            )

        admin_cursor.close()
        admin_conn.close()

        # ====================================================
        # 2. Connect to weatherdb
        # ====================================================

        write_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_TARGET_DB
        )

        write_cursor = write_conn.cursor()

        # ====================================================
        # 3. Create weather table
        # ====================================================

        write_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather(
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(50),
                time TIMESTAMP,
                wind INT,
                relative_humidity INT,
                temperature INT,

                UNIQUE(city_name, time)
            );
            """
        )

        write_conn.commit()

        # ====================================================
        # 4. Insert data
        # ====================================================

        insert_query = """
            INSERT INTO weather(
                city_name,
                time,
                wind,
                relative_humidity,
                temperature
            )
            VALUES (%s, %s, %s, %s, %s)

            ON CONFLICT (city_name, time)
            DO NOTHING;
        """

        # Insert in chunks
        chunksize = 100

        for i in range(0, len(data), chunksize):

            chunk = data[i:i + chunksize]

            values = [
                (
                    record["city_name"],
                    record["time"],
                    record["wind"],
                    record["relative_humidity"],
                    record["temperature"]
                )
                for record in chunk
            ]

            write_cursor.executemany(
                insert_query,
                values
            )

            write_conn.commit()

            print(
                f"Inserted records "
                f"{i} to {i + len(chunk) - 1}"
            )

        # ====================================================
        # 5. Close connection
        # ====================================================

        write_cursor.close()
        write_conn.close()

        print("Congratulations! Task is done completely.")

        return "Congratulations! Task is done completely"


    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    first = Extract()

    second = Transform(first)

    third = Load(second)

    # This line is technically unnecessary because
    # passing first -> second -> third already creates
    # the dependencies.
    #
    # first >> second >> third


# ============================================================
# Instantiate DAG
# ============================================================

instance = DAG()