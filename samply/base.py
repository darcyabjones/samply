"""
"""

import pandas as pd

from samply import database as db


class SamplyBase(object):

    def __init__(self, engine):
        self.engine = engine
        return

    def get_session(self):
        return db.session_scope(self.engine)

    def dump(self):
        with self.get_session() as session:
            records = session.query(self.table).all()
            rows = [self.record_to_series(r) for r in records]

        return pd.DataFrame.from_records(rows)

    def add_record(self, **kwargs):
        record = self.table(**kwargs)
        with self.get_session() as session:
            session.add(record)

    def add_records(self, records):
        with self.get_session() as session:
            session.add_all((self.table(**r) for r in records))

    def add_file(self, path):
        table = pd.read_table(path)
        self.add_table(table)
        return

    def add_table(self, table):
        records = []

        for i, loc in table.iterrows():
            this_loc = self._from_series(loc)
            records.append(this_loc)

        self.add_records(records)
        return
