"""
This package contains a lot of enumerated types that we use as controlled
vocabularies for the database. This should help make queries easier and
enforce consistency.
"""

import enum

class SampleType(enum.Enum):
    """ """
    mixed = enum.auto()
    sample = enum.auto()
    mutant = enum.auto()
    subculture = enum.auto()
    progeny = enum.auto()
    other = enum.auto()


class SamplePermission(enum.Enum):
    noncommercial = enum.auto()
    private = enum.auto()
    commercial = enum.auto()
    commercial_paid = enum.auto()
    unspecified = enum.auto()
    other = enum.auto()


class SampleContributionPredicate(enum.Enum):
    collected_by = enum.auto() # field samples
    created_by = enum.auto() # lab samples
    owned_by = enum.auto() # e.g. DAFWA etc
    entry_created_by = enum.auto() # keep track of database maintainers
    entry_modified_by = enum.auto()


class ContributorType(enum.Enum):
    person = enum.auto()
    organisation = enum.auto()


class LocationType(enum.Enum):
    point = enum.auto()
    polygon = enum.auto()
    address = enum.auto()
    town = enum.auto()
    region = enum.auto()
    state = enum.auto()
    country = enum.auto()
    other = enum.auto()


class LocationHistoryType(enum.Enum):
    pesticide = enum.auto()
    fertiliser = enum.auto()
    general_chemical = enum.auto()
    tilled = enum.auto()
    stubble_burnt = enum.auto()
    general_culture = enum.auto()
    crop = enum.auto()
    pests = enum.auto()
    land_use = enum.auto()
    disease_symptoms = enum.auto()
    stress_symptoms = enum.auto()
    general_phenotype = enum.auto()
    temperature = enum.auto()
    rainfall = enum.auto()
    general_environment = enum.auto()


class PhenotypeType(enum.Enum):
    virulent = enum.auto()
    avirulent = enum.auto()
    symptoms = enum.auto()
    invitro_growth_rate = enum.auto()
    invitro_visual_characteristics = enum.auto()
    ec50 = enum.auto()
    asexual_sporulation = enum.auto()
    sexual_sporulation = enum.auto()
    other = enum.auto()


class DateResolution(enum.Enum):
    day = enum.auto()
    week = enum.auto()
    month = enum.auto()
    quarter = enum.auto()
    season = enum.auto()
    year = enum.auto()
    decade = enum.auto()
