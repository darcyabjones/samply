"""
"""

import json
import logging

import pandas as pd
from geoalchemy2 import shape

from samply import vocabularies as voc
from samply import database as db
from samply.base import SamplyBase
from samply import utils

logger = logging.getLogger(__name__)


@utils.log(logger, logging.DEBUG)
def point_to_poly(n, e, d=0.5):
    """ Converts a gps point to a padded polygon """
    from shapely.geometry import Point
    x = Point(e, n)
    y = x.buffer(distance=d)
    return "SRID=4326;" + y.wkt


class Samples(SamplyBase):

    table = db.Samples

    @staticmethod
    def _to_series(record):
        data = [
            record.id,
            ";".join(record.names),
            record.type.name,
            record.date,
            record.date_resolution.name,
            json.dumps(record.details),
            record.permission.name,
            ";".join(p.id for p in record.parents),
            shape.to_shape(record.geom),
            record.location_type.name,
            record.location_support.get("latitude"),
            record.location_support.get("longitude"),
            record.location_support.get("street_address"),
            record.location_support.get("suburb"),
            record.location_support.get("state"),
            record.location_support.get("country"),
            json.dumps({
                k: v
                for k, v
                in record.location_support
                if k not in ("street_address", "suburb", "state", "country")
            }),
        ]

        names = [
            "id",
            "names",
            "type",
            "date",
            "date_resolution",
            "details"
            "permission",
            "parents"
            "geom",
            "location_type",
            "latitude",
            "longitude",
            "street_address",
            "suburb",
            "state",
            "country",
            "location_support",
        ]

        return pd.Series(data, index=names)

    @staticmethod
    def _from_series(series):
        record = {}
        if isinstance(series["names"], str):
            record["names"] = series["names"].split(";")
        elif isinstance(series["names"], list):
            record["names"] = series["names"]
        else:
            record["names"] = []

        record["date"] = series["date"]
        record["date_resolution"] = voc.DateResolution[
            series["date_resolution"]]
        record["details"] = json.loads(series["details"])
        record["permission"] = voc.SamplePermission[series["permission"]]

        if isinstance(series["parents"], str):
            record["parents"] = series["parents"].split(";")
        elif isinstance(series["parents"], list):
            record["parents"] = series["parents"]
        else:
            record["parents"] = []

        if isinstance(series["location_support"], dict):
            location_support = series["location_support"]
        elif isinstance(series["location_support"], str):
            location_support = json.loads(series.get("location_support", "{}"))
        else:
            location_support = {}

        for field in ("latitude", "longitude", "street_address",
                      "suburb", "state", "country"):
            if pd.notna(series[field]):
                location_support[field] = series[field]

        record["names"] = location_support

        if pd.notna(series["geom"]):
            record["geom"] = series["geom"]
        else:
            record["geom"] = point_to_poly(series["latitude"],
                                           series["longitude"])

        record["location_type"] = voc.LocationType[series["location_type"]]
        return record
