"""
"""

import logging
import json
from collections import defaultdict
import pandas as pd

from samply import utils
from samply import database as db
from samply.base import SamplyBase

logger = logging.getLogger(__name__)


class Taxon(SamplyBase):

    """
    Serialise and deserialise taxonomy database.
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
            parent_taxid=series["parent_taxid"],
            rank=series["rank"],
        )

    def add_records(self, records):
        """ Recursively add records to database """

        logger.info("Processing taxon file.")

        root = None
        nodes = defaultdict(list)
        for record in records:
            record = utils.tidy_nans(record)
            node = self.table(**record)
            if node.taxid == 1:
                root = node
            else:
                nodes[node.parent_taxid].append(node)

        logger.info("Constructing tree.")

        def recurse(node, memo):
            for child in memo[node.taxid]:
                node.children.append(recurse(child, memo))
            return node

        tree = recurse(root, nodes)

        logger.info("Adding to database.")
        with self.get_session() as session:
            session.add(tree)
        return


class SampleTaxon(SamplyBase):

    """
    Serialise and deserialise sample taxon database.
    """

    table = db.SampleTaxon

    @staticmethod
    def _to_series(record):

        data = [
            record.sample.id,
            record.taxon.taxid,
            record.type.name,
            json.dumps(record.evidence)
        ]

        names = [
            "sample_id",
            "taxid",
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
            sample_id=series["sample_id"],
            taxid=series["taxid"],
            type=series["type"],
            evidence=evidence
        )

    def add_records(self, records):
        """ Recursively add records to database """

        with self.get_session() as session:
            for record in records:
                taxid = record.pop("taxid")
                sample_id = record.pop("sample_id")

                record = utils.tidy_nans(record)
                sample = session.query(db.Sample).filter(
                    db.Sample.id == sample_id).one()
                taxon = session.query(db.Taxon).filter(
                    db.Taxon.taxid == taxid).one()

                record["taxon"] = taxon
                record["sample"] = sample
                record = self.table(**record)
                session.add(record)
        return
