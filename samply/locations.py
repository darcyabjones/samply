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


class Locations(SamplyBase):

    """
    """

    table = db.Location

    @staticmethod
    def _to_series(record):
        geom = shape.to_shape(record.geom)
        name = record.support.get("name")
        street_address = record.support.get("street_address")
        suburb = record.support.get("suburb")
        state = record.support.get("state")
        country = record.support.get("country")

        aliases = record.support.get("aliases")
        aliases = ";".join(aliases) if aliases is not None else None

        support = {
            k: v
            for k, v
            in record.support.items()
            if k not in ("name", "aliases", "street_address",
                         "suburb", "state", "country")
        }

        data = [
            name,
            record.type.name,
            aliases,
            geom,
            None,
            None,
            street_address,
            suburb,
            state,
            country,
            json.dumps(support),
            ]

        names = [
            "name",
            "type",
            "aliases",
            "geom",
            "lat",
            "lon",
            "street_address",
            "suburb",
            "state",
            "country",
            "support",
        ]
        return pd.Series(data, index=names)

    @staticmethod
    def _from_series(series):
        if isinstance(series["support"], dict):
            support = series["support"]
        elif isinstance(series["support"], str):
            support = json.loads(series.get("support", "{}"))
        else:
            support = {}

        for field in ("street_address", "suburb", "state", "country", "name"):
            if pd.notna(series[field]):
                support[field] = series[field]

        if pd.notna(series["aliases"]):
            support["aliases"] = series["aliases"].strip().split(";")

        if pd.notna(series["geom"]):
            geom = series["geom"]
        else:
            geom = point_to_poly(series["lat"], series["lon"])

        type_ = voc.LocationType[series["type"]]
        return dict(geom=geom, type=type_, support=support)


class LocationHistories(SamplyBase):

    table = db.LocationHistory

    @staticmethod
    def _to_series(record):
        return

    @staticmethod
    def _from_series(series):

        return
