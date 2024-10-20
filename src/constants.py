GEONORGE_BASE_URL = "https://nedlasting.geonorge.no/geonorge/"
GEONORGE_FIRE_STATIONS_URL = (
    GEONORGE_BASE_URL
    + "Samfunnssikkerhet/Brannstasjoner/PostGIS/Samfunnssikkerhet_0000_Norge_3035_Brannstasjoner_PostGIS.zip"
)
GEONORGE_FIRE_STATIONS_COLUMN_MAP = {
    "objid": "objid",
    "objtype": "objtype",
    "posisjon": "position",
    "brannstasjon": "fire_station",
    "brannvesen": "fire_department",
    "stasjonstype": "station_type",
}
