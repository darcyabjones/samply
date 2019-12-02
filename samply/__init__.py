__version__ = "0.0.0"

import sys
import logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

from sqlalchemy import create_engine  # noqa

from samply.cli import cli  # noqa
from samply.database import init  # noqa
from samply.contributors import Contributors  # noqa
from samply.contributors import SampleContribution  # noqa
from samply.taxon import Taxon  # noqa
from samply.taxon import SampleTaxon  # noqa
from samply.samples import Samples  # noqa
from samply.pesticides import Pesticide  # noqa
from samply.pesticides import SamplePesticide  # noqa
from samply import utils  # noqa

logger = logging.getLogger("samply")


@utils.log(logger, logging.DEBUG)
def main():
    args = cli(prog=sys.argv[0], args=sys.argv[1:])
    if args.db is None:
        print(("Please provide the db via the --db"
               "flag or the SAMPLY_DB environment variable"),
              file=sys.stderr)
        sys.exit(1)

    if args.verbose > 1:
        log_level = logging.DEBUG
    elif args.verbose > 0:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING

    logger.setLevel(log_level)

    if args.command == "init":
        init(args.db)
    elif args.command == "add":
        run_add(args)
    elif args.command == "dump":
        run_dump(args)
    else:
        print("NO SUBCOMMAND USED")
        print(args)
        sys.exit(1)


@utils.log(logger, logging.DEBUG)
def get_table(table):
    if table == "samples":
        return Samples
    elif table in ("contr", "contributors"):
        return Contributors
    elif table == "samplecontribution":
        return SampleContribution
    elif table == "taxon":
        return Taxon
    elif table == "sampletaxon":
        return SampleTaxon
    elif table == "pesticides":
        return Pesticide
    elif table == "samplepesticides":
        return SamplePesticide
    else:
        raise ValueError("No table provided")


@utils.log(logger, logging.DEBUG)
def run_add(args):
    engine = create_engine(args.db)

    table = get_table(args.table)

    tab = table(engine)
    tab.add_file(args.file)
    return


@utils.log(logger, logging.DEBUG)
def run_dump(args):
    engine = create_engine(args.db)

    table = get_table(args.table)

    tab = table(engine)
    df = tab.dump()
    df.to_csv(args.output, index=False, sep="\t")
    return
