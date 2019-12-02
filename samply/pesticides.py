"""
"""

import logging
from collections import defaultdict
from datetime import datetime
import pandas as pd

from samply import utils
from samply import vocabularies as voc
from samply import database as db
from samply.base import SamplyBase

logger = logging.getLogger(__name__)


def array_enum(string, enum):
    arr = array(string)

    # Will keyerror if invalid
    return [enum[e].name for e in arr]


def array(string):
    if isinstance(string, str):
        return string.split(";")
    elif isinstance(string, list):
        return string
    else:
        return []


class Pesticide(SamplyBase):

    """
    Serialise and deserialise pesticide database.
    """

    table = db.Pesticide

    @staticmethod
    def _to_series(record):

        data = [
            record.name,
            ";".join(n.name for n in record.pesticide_type),
            record.type,
            ";".join(record.group),
            record.notes,
            ";".join(p.name for p in record.parents),
            ]

        names = [
            "name",
            "pesticide_type",
            "type",
            "group",
            "notes",
            "parents",
        ]
        return pd.Series(data, index=names)

    @staticmethod
    def _from_series(series):
        pesticide_type = array_enum(
            series["pesticide_type"],
            voc.PesticideType
        )

        return dict(
            name=series["name"].strip(),
            pesticide_type=pesticide_type,
            type=voc.PesticideProductType[series["type"]],
            group=array(series["group"]),
            notes=series["notes"],
            parents=array(series["parents"]),
        )

    def add_records(self, records):
        """ Recursively add records to database """

        roots = []
        nodes = defaultdict(list)
        seen = set()
        for record in records:
            parents = record.pop("parents")
            record = utils.tidy_nans(record)
            node = self.table(**record)
            seen.add(node.name)
            if len(parents) == 0:
                roots.append(node)
            else:
                for parent in parents:
                    nodes[parent].append(node)

        for n in nodes.keys():
            assert (n in seen), f"{n} not in nodes"

        def recurse(node, memo):
            for child in memo[node.name]:
                node.children.append(recurse(child, memo))
            return node

        trees = [recurse(p, nodes) for p in roots]

        with self.get_session() as session:
            session.add_all(trees)
        return


class SamplePesticide(SamplyBase):

    """
    Serialise and deserialise sample taxon database.
    """

    table = db.SamplePesticide

    @staticmethod
    def _to_series(record):

        data = [
            record.sample.id,
            record.pesticide.name,
            record.date.strftime("%Y-%m-%d"),
            record.date_resolution.name,
            record.rate,
            record.units,
            record.application_style.name,
            record.stage_applied,
            record.notes,
        ]

        names = [
            "sample_id",
            "pesticide_name",
            "date",
            "date_resolution",
            "rate",
            "units",
            "application_style",
            "stage_applied",
            "notes",
        ]
        return pd.Series(data, index=names)

    @staticmethod
    def _from_series(series):
        if pd.isna(series["rate"]):
            rate = None
        else:
            rate = float(series["rate"])

        return dict(
            sample_id=series["sample_id"].strip(),
            pesticide_name=series["pesticide_name"].strip().lower(),
            date=datetime.strptime(series["date"], "%Y-%m-%d").date(),
            date_resolution=voc.DateResolution[series["date_resolution"]],
            rate=rate,
            units=series["units"],
            application_style=voc.PesticideApplication[
                series["application_style"]],
            stage_applied=series["stage_applied"],
            notes=series["notes"],
        )

    def add_records(self, records):
        """ Recursively add records to database """

        with self.get_session() as session:
            for record in records:
                record = utils.tidy_nans(record)
                pesticide_name = record.pop("pesticide_name")
                sample_id = record.pop("sample_id")

                print(pesticide_name, sample_id)
                sample = session.query(db.Sample).filter(
                    db.Sample.id == sample_id).one()
                pesticide = session.query(db.Pesticide).filter(
                    db.Pesticide.name == pesticide_name).one()

                record["pesticide"] = pesticide
                record["sample"] = sample
                record = self.table(**record)
                session.add(record)
        return
