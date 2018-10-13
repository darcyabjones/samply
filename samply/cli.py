""" Command line interface """

import os
import sys
import argparse
import logging

from samply import utils
from samply import __version__

logger = logging.getLogger("cli")


@utils.log(logger, logging.DEBUG)
def cli(prog, args):
    """ Process command line arguments.
    Often this is simply put in the main function or the
    if __name__ == "__main__" block, but keeping it as a function allows
    testing if we had more complex command line interfaces.
    Keyword arguments:
    prog -- The name of the program. Usually this will be the first element of
        the sys.argv list.
    args -- The command line arguments for the program. Usually this will be
        a slice from i.e sys.argv[1:].
    Returns:
    An argparse Args object containing the parsed args.
    Raises:
    Standard argparse exceptions if the CLI conditions are not met.
    Required parameters, type constraints etc.
    """

    parser = argparse.ArgumentParser(
        prog=prog,
        description="Project description."
        )

    parser.add_argument(
        "-d", "--db",
        type=str,
        help="The url with username and password to connect to database",
        default=os.environ.get("SAMPLY_DB", None),
    )

    # This one is interesting, a user can provide this flag multiple times,
    # and the number of times it's specified is counted, giving the value.
    # I've only ever seen it used to control the "verbosity" of running updates
    # or logging.
    parser.add_argument(
        "-v", "--verbose",
        help=("Print progress updates to stdout. Use twice (i.e. -vv or -v -v)"
              "to show debug output."),
        action="count",
        default=0
        )

    # This is also a special action, that just prints the software name and
    # version, then exits.
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s " + __version__
        )

    subparsers = parser.add_subparsers(help="subcommand help", dest="command")
    _ = cli_init(subparsers)
    _ = cli_add(subparsers)
    _ = cli_dump(subparsers)
    return parser.parse_args(args)


def cli_init(parser):
    init = parser.add_parser("init", help="Create a new database")
    return init


def cli_add(parser):
    add = parser.add_parser("add", help="Add data to the database")
    add.add_argument(
        "table",
        type=str,
        choices=["locations", "loc", "contributors", "contr", "taxon"],
        help=""
    )
    add.add_argument(
        "file",
        type=argparse.FileType('r'),
        help="The file you want to add to the database."
    )
    return add


def cli_dump(parser):
    dump = parser.add_parser("dump", help="Dump data from the database")
    dump.add_argument(
        "table",
        type=str,
        choices=["locations", "loc", "contributors", "contr", "taxon"],
        help=""
    )
    dump.add_argument(
        "-o", "--output",
        type=argparse.FileType('w'),
        default=sys.stdout,
        help="The file that you want to dump to. Default = stdout",
    )
    return dump


"""
    # For file IO we're using some special syntax from argparse.
    # Rather than having to open and close the files, handle exceptions, or
    # check that the files exist, we just let argparse do all of that for us.
    # It also enables the standard alias for stdin/stdout '-' by default.
    parser.add_argument(
        "-1", "--seq1",
        required=True,
        type=argparse.FileType('r'),
        help="FASTA file 1. Use '-' for stdin.",
        )

    parser.add_argument(
        "-2", "--seq2",
        required=True,
        type=argparse.FileType('r'),
        help="FASTA file 2. Use '-' for stdin.",
        )

    parser.add_argument(
        "-o", "--output",
        type=argparse.FileType('w'),
        default=sys.stdout,
        help="Output fasta file path. Default stdout.",
        )

    parser.add_argument(
        "-m", "--match-score",
        dest="match_score",
        type=float,
        default=1,
        help="The score to assign for matching base pairs. Default = 1."
        )

    parser.add_argument(
        "-n", "--mismatch-score",
        dest="mismatch_score",
        type=float,
        default=-1,
        help="The score to assign for mis-matching base pairs. Default = -1."
        )

    parser.add_argument(
        "-i", "--indel-score",
        dest="indel_score",
        type=float,
        default=-1,
        help="The score to assign for a gap insertion. Default = -1."
        )
        """
