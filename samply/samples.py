"""
"""

import json
import logging

import pandas as pd

from samply import database as db
from samply.base import SamplyBase
from samply import utils

logger = logging.getLogger(__name__)


class Samples(SamplyBase):

    table = db.Samples

    @staticmethod
    def _to_series(record):
        return

    @staticmethod
    def _from_series(record):
        return

