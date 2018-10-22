"""
"""

from __future__ import unicode_literals

import logging
from contextlib import contextmanager

from sqlalchemy import Table
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
# from sqlalchemy import Boolean
from sqlalchemy import Enum
from sqlalchemy import Date
from sqlalchemy import DateTime
# from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship
from sqlalchemy.orm import backref
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import JSONB

from geoalchemy2 import Geometry


from samply import vocabularies as vocab


logger = logging.getLogger(__name__)
logger.debug("Loaded module.")


SAMPLE_ID_SIZE = 10


@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    session = sessionmaker(engine)()
    try:
        yield session
        session.commit()
    except:  # noqa
        session.rollback()
        raise
    finally:
        session.close()


def init(db):
    engine = create_engine(db)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


class BaseTable(object):
    """ All tables will have these characteristics. """

    # Always name the table as the class name in lower case.
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # Always have dedicated primary key
    # id = Column(Integer, primary_key=True, autoincrement=True)

    # Give some sane defaults for printing.
    def __repr__(self):
        name = self.__class__.__name__
        columns = [c.name for c in self.__table__.columns]
        values = [getattr(self, c) for c in columns]
        kwargs = ["{}={}".format(k, v) for k, v in zip(columns, values)]
        return "{}({})".format(name, ", ".join(kwargs))

    def __str__(self):
        return repr(self)


# The Base is what we derive our other tables from.
# This handles the "metadata" attributes for us.
Base = declarative_base(cls=BaseTable)


"""
We use an adjacency table to model a many to many hierarchical relationship
between samples. For example, you might have a child isolate that is a
mutant of a parent, or you might have the projeny of two parents.
Normally you wouldn't interact with this table directly
"""
sampleadjacency = Table(
    "sampleadjacency", Base.metadata,
    Column("child_id",
           String(SAMPLE_ID_SIZE),
           ForeignKey("sample.id"),
           primary_key=True),
    Column("parent_id",
           String(SAMPLE_ID_SIZE),
           ForeignKey("sample.id"),
           primary_key=True)
)


pesticideadjacency = Table(
    "pesticideadjacency", Base.metadata,
    Column("child_id", Integer, ForeignKey("pesticide.id"), primary_key=True),
    Column("parent_id", Integer, ForeignKey("pesticide.id"), primary_key=True)
)


class Sample(Base):

    """
    """

    id = Column(String(SAMPLE_ID_SIZE), primary_key=True)
    names = Column(JSONB(none_as_null=True))  # Array
    type = Column(Enum(vocab.SampleType))

    date = Column(Date())
    date_resolution = Column(Enum(vocab.DateResolution))

    details = Column(JSONB(none_as_null=True))
    permission = Column(Enum(vocab.SamplePermission))

    geom = Column(Geometry(geometry_type="POLYGON", srid=4326))
    location_type = Column(Enum(vocab.LocationType))
    location_support = Column(JSONB(none_as_null=True))  # address?, postcode?

    contributions = relationship("SampleContribution", back_populates="sample")
    phenotypes = relationship("Phenotype", back_populates="sample")
    parents = relationship(
        "Sample",
        secondary=sampleadjacency,
        primaryjoin="Sample.id==sampleadjacency.c.child_id",
        secondaryjoin="Sample.id==sampleadjacency.c.parent_id",
        backref="children"
    )
    taxon = relationship("SampleTaxon", back_populates="sample")
    pesticides = relationship("SamplePesticide", back_populates="sample")


class SampleTaxon(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_id = Column(String(SAMPLE_ID_SIZE),
                       ForeignKey("sample.id"),
                       primary_key=True)
    taxon_id = Column(Integer, ForeignKey("taxon.taxid"), primary_key=True)
    type = Column(Enum(vocab.SampleTaxonType))
    evidence = Column(JSONB(none_as_null=True))
    sample = relationship("Sample", back_populates="taxon")
    taxon = relationship("Taxon", back_populates="samples")


class Taxon(Base):
    taxid = Column(Integer, primary_key=True)
    name = Column(String())
    rank = Column(String())
    alt_names = Column(JSONB(none_as_null=True))
    parent_taxid = Column(Integer, ForeignKey("taxon.taxid"))
    samples = relationship("SampleTaxon", back_populates="taxon")
    children = relationship("Taxon",
                            backref=backref("parent", remote_side=[taxid]))


class SamplePesticide(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_id = Column(
        String(SAMPLE_ID_SIZE),
        ForeignKey("sample.id"),
        primary_key=True
    )
    pesticide_id = Column(
        Integer,
        ForeignKey("pesticide.id"),
        primary_key=True
    )
    date = Column(Date())  # Split into multiple columns?
    date_resolution = Column(Enum(vocab.DateResolution))
    rate = Column(Float())
    units = Column(String())
    sample = relationship("Sample", back_populates="pesticides")
    pesticide = relationship("Pesticide", back_populates="samples")


class Pesticide(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(), index=True, unique=True)
    samples = relationship("SamplePesticide", back_populates="pesticide")
    pesticide_type = Column(JSONB(none_as_null=True))
    type = Column(Enum(vocab.PesticideProductType))
    group = Column(JSONB(none_as_null=True))
    notes = Column(String())
    parents = relationship(
        "Pesticide",
        secondary=pesticideadjacency,
        primaryjoin="Pesticide.id==pesticideadjacency.c.child_id",
        secondaryjoin="Pesticide.id==pesticideadjacency.c.parent_id",
        backref="children"
    )


class SampleContribution(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_id = Column(
        String(SAMPLE_ID_SIZE),
        ForeignKey("sample.id"),
        primary_key=True
    )
    contributor_id = Column(
        Integer,
        ForeignKey("contributor.id"),
        primary_key=True
    )
    predicate = Column(Enum(vocab.SampleContributionPredicate))
    datetime = Column(DateTime())
    sample = relationship("Sample", back_populates="contributions")
    contributor = relationship("Contributor", back_populates="samples")


class Contributor(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(Enum(vocab.ContributorType))
    name = Column(String())
    # Address, email, phone, twitter etc.
    contact = Column(JSONB(none_as_null=True))
    samples = relationship("SampleContribution", back_populates="contributor")


class LocationHistory(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    geom = Column(Geometry(geometry_type="POLYGON", srid=4326))
    location_type = Column(Enum(vocab.LocationType))
    location_support = Column(JSONB(none_as_null=True))  # address?, postcode?,
    type = Column(Enum(vocab.EnvironmentalHistoryType))
    date = Column(Date())  # Split into multiple columns?
    date_resolution = Column(Enum(vocab.DateResolution))
    # {fungicide: , rates: , units: "mg/Ha"}
    details = Column(JSONB(none_as_null=True))
    location_id = Column(Integer, ForeignKey("location.id"))
    location = relationship("Location", back_populates="history")


class Phenotype(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(Enum(vocab.PhenotypeType))
    date = Column(Date())
    details = Column(JSONB(none_as_null=True))
    sample_id = Column(String(SAMPLE_ID_SIZE), ForeignKey("sample.id"))
    sample = relationship("Sample", back_populates="phenotypes")
