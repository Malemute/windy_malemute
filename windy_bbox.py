from datetime import datetime, timedelta

import math
import pandas as pd
from pandas import json_normalize
import requests

from sqlalchemy import engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, ForeignKey, Integer, Numeric, String, DateTime
from sqlalchemy.ext.declarative import declarative_base


def open_db():
    connection = {'user': 'malemute',
                  'password': '*****',
                  'host': '127.0.0.1',
                  'port': '3306',
                  'database': 'windy_db'}

    windy_db = DBConnect(connection=connection)
    my_engine = windy_db.engine
    DeclarativeBase.metadata.create_all(my_engine)
    return my_engine


class DBConnect:

    def __init__(self, connection):

        self.engine = engine.create_engine(
            'mysql+pymysql://{}:{}@{}:{}/{}'.format(
                connection["user"],
                connection["password"],
                connection["host"],
                connection["port"],
                connection["database"]
                )
            )


DeclarativeBase = declarative_base()


class StationDb(DeclarativeBase):
    __tablename__ = 'stations'

    id = Column(Integer, primary_key=True)
    station_name = Column('station_name', String)
    latitude = Column('latitude', Numeric)
    longitude = Column('longitude', Numeric)

    def __repr__(self):
        return "".format(self.code)


class WaterLevelsDb(DeclarativeBase):
    __tablename__ = 'water_levels'

    id = Column(Integer, primary_key=True)
    station_id = Column(ForeignKey('stations.id', ondelete='CASCADE'), nullable=False, index=True)
    date_time = Column('date_time', DateTime)
    water_level = Column('water_level', Numeric)

    def __repr__(self):
        return "".format(self.code)


def build_query_url(
    begin_date,
    end_date,
    product,
    datum=None,
    interval=None,
    unit="Meters",
    time_zone="GMT",
):
    """
    Build an URL to be used to fetch data from the NOAA CO-OPS data API
    (see https://tidesandcurrents.noaa.gov/api/)
    """
    # base_url = 'https://opendap.co-ops.nos.noaa.gov/ioos-dif-sos/SOS?'
    # base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
    sos_url = "https://opendap.co-ops.nos.noaa.gov/ioos-dif-sos/SOS"
    base_url = (
            sos_url
                + "?service=SOS&request=GetObservation&version=1.0.0"
                + "&observedProperty=water_surface_height_above_reference_datum"
    )


    # If the data product is water levels, check that a datum is specified
    if product == "water_level":
        if datum is None:
            raise ValueError(
                "No datum specified for water level data. See"
                " https://tidesandcurrents.noaa.gov/api/#datum "
                "for list of available datums"
            )
        else:
            # Compile parameter string for use in URL
            parameters = {
                "observedProperty": "water_surface_height_above_reference_datum",
                # check begin and end
                "eventTime": "{}Z/{}Z".format(begin_date, end_date),
                "offering":"urn:ioos:network:NOAA.NOS.CO-OPS:WaterLevelActive",
                "featureOfInterest": "BBOX:-177.3600,-14.2767,167.7361,70.4114",
                # "featureOfInterest": "BBOX:-177.3600,-14.2767,167.7361,0.0000",
                "responseFormat":"text/csv",
                "result":"VerticalDatum%3D%3Durn:ioos:def:datum:noaa::{}".format(datum),
                "unit": unit,
                "dataType": "PreliminarySixMinute",
                "timeZone": time_zone,
                # "application": "py_noaa",
                # "format": "json",
            }

    # For all other data types (e.g., meteoroligcal conditions)
    else:
        # If no interval provided, return 6-min met data
        if interval is None:
            # Compile parameter string for use in URL
            parameters = {
                "begin_date": begin_date,
                "end_date": end_date,
                # "station": self.stationid,
                "product": product,
                "units": unit,
                "time_zone": time_zone,
                "application": "py_noaa",
                "format": "json",
            }
        else:
            # Compile parameter string, including interval, for use in URL
            parameters = {
                "begin_date": begin_date,
                "end_date": end_date,
                # "station": self.stationid,
                "product": product,
                "interval": interval,
                "units": unit,
                "time_zone": time_zone,
                "application": "py_noaa",
                "format": "json",
            }

    # Build URL with requests library
    query_url = (
        requests.Request("GET", base_url, params=parameters).prepare().url
    )

    return query_url


def get_water_levels_from_noaa():

    today = datetime.utcnow().replace(microsecond=0)
    delta_past = timedelta(hours=PREDICTION_DEPTH)
    # delta = timedelta(days=PREDICTION_DEPTH)
    # future = today + delta
    past = today - delta_past

    noaa_url = build_query_url(
        begin_date=past.isoformat(),
        end_date=today.isoformat(),
        product="water_level",
        # product="predictions",
        datum="MLLW",
        unit="Meters",
        time_zone="GMT",
        # interval='h',
    )

    tide_table = requests.get(noaa_url).text

    put_water_levels_to_db(tide_table)


def put_water_levels_to_db(tide_table):

    # open database
    the_engine = open_db()
    Session = sessionmaker(bind=the_engine)
    session = Session()

    tide_measures_list = tide_table.split('\n')

    for measure_row in tide_measures_list[1:]:
        if not measure_row:
            continue
        measure = measure_row.split(',')
        station_full = measure[0]
        station_id = station_full.split(':')[-1]

        # put measure to database
        new_measure_in_db = WaterLevelsDb(
                station_id=int(station_id),
                date_time=datetime.fromisoformat(measure[4][:-1]),
                water_level=float(measure[5]),
                )
        # Добавляем запись
        session.add(new_measure_in_db)

    session.commit()
    print(tide_measures_list[-5:])


if __name__ == "__main__":

    PREDICTION_DEPTH = 1

    get_water_levels_from_noaa()
