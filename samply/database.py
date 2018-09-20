"""
Functions to count the number of specific classes of CAZymes.
"""

from __future__ import unicode_literals

import re
import enum
import logging
from contextlib import contextmanager

from sqlalchemy import Table
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import and_
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy import Enum
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import JSONB

from geoalchemy2 import Geometry


from samply import vocabularies as vocab




logger = logging.getLogger(__name__)
logger.debug("Loaded module.")


@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    session = sessionmaker(engine)()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class BaseTable(object):
    """ All tables will have these characteristics. """

    # Always name the table as the class name in lower case.
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # Always have dedicated primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

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


# We use an adjacency table to model a many to many hierarchical relationship
# between samples.
# For example, you might have a child isolate that is a mutant of a parent,
# or you might have the projeny of two parents.
# Normally you wouldn't interact with this table directly
sampleadjacency = Table(
    "sampleadjacency", Base.metadata,
    Column("child_id", Integer, ForeignKey("sample.id"), primary_key=True),
    Column("parent_id", Integer, ForeignKey("sample.id"), primary_key=True)
)


# We use another adjacency table to map a many to many relationship between
# samples and phenotypes.
# Again, you would use the sample or phenotype tables to actually interact
# this just handles it behind the scenes.
samplephenotype = Table(
    "samplephenotype", Base.metadata,
    Column("sample_id", Integer, ForeignKey("sample.id"), primary_key=True),
    Column("phenotype_id", Integer, ForeignKey("phenotype.id"), primary_key=True)
)


class Sample(Base):
    """
    """

    name = Column(String(50), index=True, nullable=False, unique=True)
    alt_names = Column(JSONB(none_as_null=True)) # Array
    type = Column(Enum(vocab.SampleType))

    taxon = Column(JSONB(none_as_null=True)) # Bunch of dbxrefs
    taxon_evidence = Column(JSONB(none_as_null=True))

    date = Column(Date()) #Split into multiple columns?
    date_resolution = Column(Enum(vocab.DateResolution))
    details = Column(JSONB(none_as_null=True)) # e.g. host = blah, tissue= stem,...
    permission = Column(Enum(vocab.SamplePermission))
    location_id = Column(Integer, ForeignKey("location.id"))
    location = relationship("Location", back_populates="samples")
    contributions = relationship("SampleContribution", back_populates="sample")
    phenotypes = relationship("Phenotype", secondary=samplephenotype, back_populates="samples")
    parents = relationship(
        "Sample",
        secondary=sampleadjacency,
        primaryjoin="Sample.id==sampleadjacency.c.child_id",
        secondaryjoin="Sample.id==sampleadjacency.c.parent_id",
        backref="children"
    )


class SampleContribution(Base):
    sample_id = Column(Integer, ForeignKey("sample.id"), primary_key=True)
    contributor_id = Column(Integer, ForeignKey("contributor.id"), primary_key=True)
    predicate = Column(Enum(vocab.SampleContributionPredicate))
    datetime = Column(DateTime())
    sample = relationship("Sample", back_populates="contributions")
    contributor = relationship("Contributor", back_populates="samples")


class Contributor(Base):
    type = Column(Enum(vocab.ContributorType))
    name = Column(String())
    contact = Column(JSONB(none_as_null=True)) # Address, email, phone, twitter etc.
    samples = relationship("SampleContribution", back_populates="contributor")


class Location(Base):
    geom = Column(Geometry(geometry_type="POLYGON", srid=4326))
    type = Column(Enum(vocab.LocationType))
    support = Column(JSONB(none_as_null=True)) # address?, postcode?,
    samples = relationship("Sample", back_populates="location")
    history = relationship("LocationHistory", back_populates="location")


class LocationHistory(Base):
    type = Column(Enum(vocab.LocationHistoryType))
    date = Column(Date()) #Split into multiple columns?
    details = Column(JSONB(none_as_null=True)) # {fungicide: , rates: , units: "mg/Ha"}
    location_id = Column(Integer, ForeignKey("location.id"))
    location = relationship("Location", back_populates="history")


class Phenotype(Base):
    type = Column(Enum(vocab.PhenotypeType))
    date = Column(Date())
    details = Column(JSONB(none_as_null=True))
    samples = relationship("Sample", secondary=samplephenotype, back_populates="phenotypes")

