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


class Environments(SamplyBase):

    """
    """

    table = db.Environment

    @staticmethod
    def _to_series(record):
        samples = ";".record

        type_ = record.type.name
        date = record.date
        date_resolution = record.date_resolution.name

        details = record.details
        crop = record.details.pop("crop", None)
        cultivar = record.details.pop("cultivar", None)
        tissue = record.details.pop("tissue", None)

        fungicide = record.details.pop("fungicide", None)
        rate = record.details.pop("rate", None)
        units = record.details.pop("units", None)
        application_style = record.details.pop("application_style", None)

        data = [
            samples,
            type_,
            date,
            date_resolution,
            crop,
            cultivar,
            tissue,
            fungicide,
            rate,
            units,
            application_style,
            json.dumps(details),
        ]
        names = [
            "samples",
            "type",
            "date",
            "date_resolution",
            "crop",
            "cultivar",
            "tissue",
            "fungicide",
            "rate",
            "units",
            "application_style",
            "details",
        ]
        return pd.Series(data, index=names)

    @staticmethod
    def _from_series(series):
        if isinstance(series["details"], dict):
            details = series["details"]
        else:
            details = json.loads(series.get("details", "{}"))

        for field in ("crop", "cultivar", "tissue", "fungicide", "rate",
                      "units", "application_style",):
            if pd.notna(series[field]):
                details[field] = series[field]

        samples = series["samples"].split(";")
        type_ = voc.EnvironmentalHistoryType[series["type"]]

        date = series["date"]
        date_resolution = voc.DateResolution[series["date_resolution"]]

        return dict(samples=samples, type=type_, date=date,
                    date_resolution=date_resolution, details=details)
