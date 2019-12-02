"""
"""

import json
import logging
from datetime import datetime

import pandas as pd
from samply import database as db
from samply.base import SamplyBase
from samply import vocabularies as voc
from samply import utils

logger = logging.getLogger(__name__)


class Contributors(SamplyBase):

    """
    """

    table = db.Contributor

    @staticmethod
    def _to_series(record):
        type_ = record.type.name
        name = record.name

        phone = record.contact.get("phone")
        email = record.contact.get("email")

        contact = {
            k: v
            for k, v
            in record.contact.items()
            if k not in ("phone", "email")
        }

        series = pd.Series(
            [type_, name, email, str(phone), json.dumps(contact)],
            index=["type", "name", "email", "phone", "contact"]
        )
        return series

    @staticmethod
    def _from_series(series):
        record = {
            "type": series["type"],
            "name": series["name"],
            "contact": {},
        }

        if pd.notna(series.contact):
            if isinstance(series.contact, dict):
                record["contact"] = series.contact
            elif isinstance(series.contact, str):
                record["contact"] = json.loads(series.contact)
            else:
                record["contact"] = {}

        if pd.notna(series.phone):
            record["contact"]["phone"] = str(series.phone)

        if pd.notna(series.email):
            record["contact"]["email"] = series.email

        return record


class SampleContribution(SamplyBase):

    """
    Serialise and deserialise sample taxon database.
    """

    table = db.SampleContribution

    @staticmethod
    def _to_series(record):

        data = [
            record.sample.id,
            record.contributor.name,
            record.predicate.name,
            record.datetime.strftime("%Y-%m-%d"),
        ]

        names = [
            "sample_id",
            "contributor_name",
            "predicate",
            "datetime",
        ]
        return pd.Series(data, index=names)

    @staticmethod
    def _from_series(series):
        return dict(
            sample_id=series["sample_id"],
            contributor_name=series["contributor_name"],
            predicate=series["predicate"],
            datetime=datetime.strptime(series["datetime"], "%Y-%m-%d"),
        )

    def add_records(self, records):
        """ Recursively add records to database """

        with self.get_session() as session:
            for record in records:
                record = utils.tidy_nans(record)
                sample_id = record.pop("sample_id")
                contributor_name = record.pop("contributor_name")

                sample = session.query(db.Sample).filter(
                    db.Sample.id == sample_id).one()
                contributor = session.query(db.Contributor).filter(
                    db.Contributor.name == contributor_name).one()

                record["contributor"] = contributor
                record["sample"] = sample
                record = self.table(**record)
                session.add(record)
        return
