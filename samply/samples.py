"""
"""

import json
import logging
from collections import defaultdict
from datetime import datetime

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


def json_serialise(val):
    if isinstance(val, str):
        return json.loads(val)
    elif isinstance(val, dict):
        return val
    else:
        return {}


def array_serialise(val):
    if isinstance(val, str):
        return val.split(";")
    elif isinstance(val, list):
        return val
    else:
        return []


class Samples(SamplyBase):

    table = db.Sample

    @staticmethod
    def _to_series(record):
        data = [
            record.id,
            ";".join(record.names),
            record.type.name,
            record.date.strftime("%Y-%m-%d"),
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
                in record.location_support.items()
                if k not in ("street_address", "suburb", "state", "country")
            }),
        ]

        names = [
            "id",
            "names",
            "type",
            "date",
            "date_resolution",
            "details",
            "permission",
            "parents",
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
        record["id"] = series["id"]
        record["type"] = series["type"]
        record["names"] = array_serialise(series["names"])
        record["date"] = datetime.strptime(series["date"], "%Y-%m-%d").date()
        record["date_resolution"] = voc.DateResolution[
            series["date_resolution"]]
        record["details"] = json_serialise(series["details"])
        record["permission"] = voc.SamplePermission[series["permission"]]
        record["parents"] = array_serialise(series["parents"])
        location_support = json_serialise(series["location_support"])

        for field in ("latitude", "longitude", "street_address",
                      "suburb", "state", "country"):
            if pd.notna(series[field]):
                location_support[field] = series[field]

        for field in ("latitude", "longitude"):
            if pd.notna(series[field]):
                location_support[field] = float(series[field])

        record["location_support"] = location_support

        if pd.notna(series["geom"]):
            record["geom"] = series["geom"]
        else:
            record["geom"] = point_to_poly(
                location_support["latitude"],
                location_support["longitude"]
            )

        record["location_type"] = voc.LocationType[series["location_type"]]
        if record["id"] in record["parents"]:
            raise ValueError("{} is present as child and parent".format(
                record["id"]))
        return record

    def add_records(self, records):
        """ Recursively add records to database """

        logger.info("Processing taxon file.")

        roots = []
        nodes = defaultdict(list)
        for record in records:
            parents = record.pop("parents")
            record = utils.tidy_nans(record)
            node = self.table(**record)
            if pd.isna(parents) or len(parents) == 0:
                roots.append(node)
            else:
                for parent in parents:
                    nodes[parent].append(node)

        def recurse(node, memo):
            for child in memo[node.id]:
                node.children.append(recurse(child, memo))
            return node

        tree = [recurse(root, nodes) for root in roots]

        with self.get_session() as session:
            session.add_all(tree)
        return
