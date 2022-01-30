from datetime import datetime, timedelta

from sqlalchemy import engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, ForeignKey, Integer, Numeric, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

from noaa_stations import Station


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


def get_stations_from_db(session):
    stations_list = []
    for station in session.query(StationDb):
        stations_list.append(station)

    return stations_list


def connect_db():
    connection = {'user': 'catchthewind_dev',
                  'password': 'VJxoA7XUSbzRi5nQ',
                  'host': '172.201.0.202',
                  'port': '3306',
                  'database': 'catchthewind_dev'}

    db = DBConnect(connection=connection)
    return db


def put_tide_predictions():

    today = datetime.utcnow().replace(microsecond=0)
    # delta_past = timedelta(hours=1)
    delta = timedelta(days=PREDICTION_DEPTH)
    future = today + delta
    # past = today - delta_past

    # open database
    the_engine = open_db()
    Session = sessionmaker(bind=the_engine)
    session = Session()

    session.query(PredictionsDb).filter(PredictionsDb.date_time <= today).delete(synchronize_session='fetch')
    session.commit()

    stations_list = get_stations_from_db(session)
    # for every station from stations
    is_first_print = 0
    for station_row in stations_list:
        station_id = station_row.id
    #    get predictions
        station = Station(station_id)
    #    get water level
        tide_data = station.get_data(
            begin_date=today.strftime("%Y%m%d %H:%M"),
            end_date=future.strftime("%Y%m%d %H:%M"),
            # product="water_level",
            product="predictions",
            datum="MLLW",
            units="metric",
            time_zone="gmt",
            # interval='h',
            application='Eugene_Mamontov',
            )

        # put predictions to database
        for date_time, row in tide_data.iterrows():
            new_pred = PredictionsDb(
                station_id=station_id,
                date_time=date_time,
                predicted_wl=row['predicted_wl'],
                )
            # Добавляем запись
            session.add(new_pred)

        print(tide_data.tail())
        is_first_print += 1
        if is_first_print >= 10:
            break

    session.commit()


if __name__ == "__main__":

    PREDICTION_DEPTH = 6

    put_tide_predictions()
