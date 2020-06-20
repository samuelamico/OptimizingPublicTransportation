"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str



app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")


topic = app.topic("stations", value_type=Station)


out_topic = app.topic("cda.transformed.station", partitions=1)

# TODO: Define a Faust Table
table = app.Table(
     "cda.station.table",
     default=TransformedStation,
     partitions=1,
     changelog_topic=out_topic,
)



@app.agent(topic)
async def stationevent(stations):
    async for station in stations:
        transformStation = TransformedStation(
                station_id = station.station_id,
                station_name = station.station_name,
                order = station.order,
                line = 'red' if station.red else 'blue' if station.blue else 'green'
        )
        table[station.station_id] = transformStation
        #await out_topic.send(key=transformStation.station_id,value=clickevent)


if __name__ == "__main__":
    app.main()
