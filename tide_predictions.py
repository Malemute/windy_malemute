from datetime import datetime, timedelta

from sqlalchemy import engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, ForeignKey, Integer, Numeric, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

import noaa_stations


def open_db():
    connection = {'user': 'malemute',
                  'password': '_Gerai_',
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


class PredictionsDb(DeclarativeBase):
    __tablename__ = 'predictions'

    id = Column(Integer, primary_key=True)
    station_id = Column(ForeignKey('stations.id', ondelete='CASCADE'), nullable=False, index=True)
    date_time = Column('date_time', DateTime)
    predicted_wl = Column('predicted_wl', Numeric)

    def __repr__(self):
        return "".format(self.code)


def get_stations_from_site():
    stations_list = []
    return stations_list


def get_stations_from_db(session_db):
    stations_list = []
    for station in session_db.query(StationDb):
        stations_list.append(station.id)

    return stations_list[:500]


def get_tide_predictions_from_noaa():

    # open database
    the_engine = open_db()
    Session = sessionmaker(bind=the_engine)
    session_db = Session()
    stations_list = get_stations_from_db(session_db)

    today = datetime.utcnow().replace(microsecond=0)
    # delta_past = timedelta(hours=1)
    delta = timedelta(days=PREDICTION_DEPTH)
    future = today + delta

    tides_by_stations = noaa_stations.get_data(
        stations_list,
        begin_date=today.strftime("%Y%m%d %H:%M"),
        end_date=future.strftime("%Y%m%d %H:%M"),
        product="predictions",
        datum="MLLW",
        )

    put_tide_predictions_to_db(session_db, tides_by_stations)


def put_tide_predictions_to_db(session_db, tides_by_stations):

    today = datetime.utcnow().replace(microsecond=0)
    session_db.query(PredictionsDb).filter(PredictionsDb.date_time <= today).delete(synchronize_session='fetch')
    session_db.commit()

        # put predictions to database
    for date_time, row in tides_by_stations.iterrows():
        new_pred = PredictionsDb(
            station_id=row['station_id'],
            date_time=date_time,
            predicted_wl=row['predicted_wl'],
            )
        # Добавляем запись
        session_db.add(new_pred)

    session_db.commit()


if __name__ == "__main__":

    PREDICTION_DEPTH = 6

    get_tide_predictions_from_noaa()
