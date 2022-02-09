import aiohttp
import asyncio

from noaa_stations import build_base_url


async def get_tide_async(base_url, stations_loc, tides_by_stations):
    conc_req = 10
    async with aiohttp.ClientSession() as session:
        for i in range(conc_req):
            if len(stations_loc) == 0:
                break
            station_id = stations_loc.pop()
            url = build_station_url(base_url, station_id)
            async with session.get(url) as resp:
                tide_row = await resp.json()
                tide_row['station_id'] = station_id
                tides_by_stations.append(tide_row)


def build_station_url(base_url, station_id):
    return '{}&station={}'.format(base_url, station_id)


def get_data_from_noaa(
        begin_date,
        end_date,
        stations_list
):

    base_url = build_base_url(
        begin_date,
        end_date,
    )
    tides_by_stations = []
    stations_loc = stations_list.copy()

    while True:
        if len(stations_loc) == 0:
            break

        asyncio.run(get_tide_async(base_url, stations_loc, tides_by_stations))

    return tides_by_stations
