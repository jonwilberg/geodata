import io
import zipfile

import pandas as pd
import requests

from src.constants import GEONORGE_FIRE_STATIONS_COLUMN_MAP, GEONORGE_FIRE_STATIONS_URL


def fetch_fire_stations_data_from_geonorge() -> str:
    """Load data on fire stations in Norway from Geonorge and return the content as a string."""
    response = requests.get(GEONORGE_FIRE_STATIONS_URL)
    response.raise_for_status()
    return _extract_text_from_response(response=response)


def _extract_text_from_response(response: requests.Response) -> str:
    """Extract the text in the zipped response content."""
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        filename = zip_file.namelist()[0]
        with zip_file.open(filename) as txt_file:
            return txt_file.read().decode("utf-8")


def parse_fire_stations_text_data_into_dataframe(
    input_text: str,
) -> pd.DataFrame:
    """Parse the text content of the Geonorge response as a DataFrame."""
    lines = input_text.split("\n")
    data_start_idx = next(i for i, s in enumerate(lines) if s.startswith("COPY"))
    data_end_idx = next(i for i, s in enumerate(lines[data_start_idx:]) if s.startswith(r"\."))
    columns_str = lines[data_start_idx].split("(")[1].split(")")[0]
    columns = [c.strip() for c in columns_str.split(",")]
    rows = [r.split("\t") for r in lines[data_start_idx + 1 : data_end_idx]]
    return pd.DataFrame(rows, columns=columns)


def preprocess_fire_stations_dataframe(
    fire_stations_df: pd.DataFrame,
) -> pd.DataFrame:
    """Remove unused columns, rename columns ensure consistent capitalization."""
    drop_columns = [c for c in fire_stations_df.columns if c not in GEONORGE_FIRE_STATIONS_COLUMN_MAP]
    fire_stations_df.drop(columns=drop_columns, inplace=True)
    fire_stations_df.rename(columns=GEONORGE_FIRE_STATIONS_COLUMN_MAP, inplace=True)
    fire_stations_df["fire_station"] = fire_stations_df["fire_station"].str.capitalize()
    return fire_stations_df


def store_fire_stations_data(fire_stations_df: pd.DataFrame) -> None:
    """Store the fire stations data locally."""
    fire_stations_df.to_csv("data/fire_stations.csv")
