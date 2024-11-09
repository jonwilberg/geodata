from typing import Any

import pendulum
from airflow.decorators import dag, task


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
        from src.load_firestations_data import fetch_fire_stations_data_from_geonorge

        return fetch_fire_stations_data_from_geonorge()

    @task(multiple_outputs=True)
    def parse_text_response_into_dataframe(input_text: str) -> Any:
        from src.load_firestations_data import parse_fire_stations_text_data_into_dataframe

        return parse_fire_stations_text_data_into_dataframe(input_text=input_text)

    @task()
    def preprocess_fire_stations_data(fire_stations_df: Any) -> Any:
        from src.load_firestations_data import preprocess_fire_stations_dataframe

        return preprocess_fire_stations_dataframe(fire_stations_df=fire_stations_df)

    @task()
    def store_fire_stations_data(fire_stations_df: Any) -> None:
        from src.load_firestations_data import store_fire_stations_dataframe

        store_fire_stations_dataframe(fire_stations_df=fire_stations_df)

    fire_stations_text = fetch_fire_stations_data()
    fire_stations = parse_text_response_into_dataframe(fire_stations_text)
    preprocessed_fire_stations = preprocess_fire_stations_data(fire_stations)
    store_fire_stations_data(preprocessed_fire_stations)


dag = geodata_dag()
