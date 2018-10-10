"""
"""

import json
import logging

import pandas as pd
from samply import vocabularies as voc
from samply import database as db
from samply.base import SamplyBase
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
            in record.contact
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
            else:
                record["contact"] = json.loads(series.contact)

        if pd.notna(series.phone):
            record["contact"]["phone"] = str(series.phone)

        if pd.notna(series.email):
            record["contact"]["email"] = series.email

        return record
