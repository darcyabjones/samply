"""
"""

import logging
import json
import pandas as pd

from samply import database as db
from samply.base import SamplyBase

logger = logging.getLogger(__name__)


class Taxon(SamplyBase):

    """
    """

    table = db.Taxon

    @staticmethod
    def _to_series(record):

        data = [
            record.name,
            record.rank,
            ";".join(record.alt_names),
            record.taxid,
            record.parent_taxid
            ]

        names = [
            "name",
            "rank",
            "alt_names",
            "taxid",
            "parent_taxid"
        ]
        return pd.Series(data, index=names)

    @staticmethod
    def _from_series(series):
        if isinstance(series["alt_names"], str):
            alt_names = series["alt_names"].split(";")
        else:
            alt_names = []

        return dict(
            name=series["name"],
            alt_names=alt_names,
            taxid=series["taxid"],
            parent_taxid=None,  # series["parent_taxid"],
            rank=series["rank"],
        )


class SampleTaxon(SamplyBase):

    """
    """

    table = db.SampleTaxon

    @staticmethod
    def _to_series(record):

        data = [
            record.sample.name,
            record.taxon.name,
            record.type.name,
            json.dumps(record.evidence)
            ]

        names = [
            "sample",
            "taxon",
            "type",
            "evidence"
        ]
        return pd.Series(data, index=names)

    @staticmethod
    def _from_series(series):
        if isinstance(series["evidence"], str):
            evidence = series["evidence"].split(";")
        else:
            evidence = []

        return dict(
            sample=series["sample"],
            taxon=series["taxon"],
            type=series["type"],
            evidence=evidence
        )
