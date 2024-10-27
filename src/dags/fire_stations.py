import pandas as pd
import pendulum
from airflow.decorators import dag, task

from src.load_firestations_data import (
    fetch_fire_stations_data_from_geonorge,
    parse_fire_stations_text_data_into_dataframe,
    preprocess_fire_stations_dataframe,
    store_fire_stations_dataframe,
)


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 10, 27, tz="UTC"),
    catchup=False,
)
def geodata_dag():
    """
    ### Geodata DAG
    A simple DAG to fetch, process, and store geodata for Norwegian fire stations.
    """

    @task()
    def fetch_fire_stations_data() -> str:
        """
        #### Fetch Fire Stations Data task
        Fetches data on fire stations in Norway from Geonorge.
        """
        return fetch_fire_stations_data_from_geonorge()

    @task(multiple_outputs=True)
    def parse_text_response_into_dataframe(input_text: str) -> pd.DataFrame:
        """
        #### Parse Text Response into DataFrame task
        Parses the text content of the Geonorge response as a DataFrame.
        """
        return parse_fire_stations_text_data_into_dataframe(input_text=input_text)

    @task()
    def preprocess_fire_stations_data(fire_stations_df: pd.DataFrame) -> pd.DataFrame:
        """
        #### Preprocess Fire Stations DataFrame task
        Removes unused columns, renames columns, and ensures consistent capitalization.
        """
        return preprocess_fire_stations_dataframe(fire_stations_df=fire_stations_df)

    @task()
    def store_fire_stations_data(fire_stations_df: pd.DataFrame) -> None:
        """
        #### Store Fire Stations Data task
        Stores the fire stations data locally.
        """
        store_fire_stations_dataframe(fire_stations_df=fire_stations_df)

    fire_stations_text = fetch_fire_stations_data()
    fire_stations = parse_text_response_into_dataframe(fire_stations_text)
    preprocessed_fire_stations = preprocess_fire_stations_data(fire_stations)
    store_fire_stations_data(preprocessed_fire_stations)


geodata_dag()
