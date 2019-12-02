#!/usr/bin/env python3

import sys
import argparse
import pandas as pd


def cli(prog, args):
    parser = argparse.ArgumentParser(
        prog=prog,
        description="convert ncbi taxonomy names and nodes to internal format"
    )

    parser.add_argument(
        "-n", "--nodes",
        required=True,
        type=argparse.FileType('r'),
        help="Input nodes file",
    )

    parser.add_argument(
        "-a", "--names",
        required=True,
        type=argparse.FileType('r'),
        help="Input names file",
    )

    parser.add_argument(
        "-c", "--custom",
        required=False,
        type=argparse.FileType('r'),
        default=None,
        help="custom taxon names, use negative integers.",
    )

    parser.add_argument(
        "-o", "--out",
        type=argparse.FileType('w'),
        default=sys.stdout,
        help="File to write to.",
    )
    return parser.parse_args(args)


def appl(df):
    sci_names = [
        r["name"]
        for i, r
        in df.iterrows()
        if r["name_class"] == "scientific name"
    ]

    name = sci_names[0] if len(sci_names) > 0 else None
    names = list(df["name"][df["name"].notnull()].unique())
    unique_names = list(
        df["unique_name"][df["unique_name"].notnull()].unique())
    alt_names = names
    alt_names.extend(unique_names)

    return pd.Series({"name": name, "alt_names": ";".join(alt_names)})


def main():
    args = cli(prog=sys.argv[0], args=sys.argv[1:])

    nodes = pd.read_table(  # noqa
        args.nodes,
        sep="\t\|\t?",
        engine="python",
        names=["taxid", "parent_taxid", "rank", "embl_code", "division_id",
               "idivflag", "gencodeid", "igencodeid", "mcodeid", "imcodeid",
               "gbkhidden", "hiddensubtree", "comments", "junk"]
    )

    names = pd.read_table(  # noqa
        args.names,
        sep="\t\|\t?",
        engine="python",
        names=["taxid", "name", "unique_name", "name_class", "junk"]
    )

    merged = pd.merge(nodes, names, on="taxid")
    taxon = merged.groupby(["taxid", "parent_taxid", "rank"]).apply(appl)
    taxon.reset_index(inplace=True, drop=False)
    taxon["alt_names"] = taxon["alt_names"]

    if args.custom is not None:
        custom_taxon = pd.read_table(args.custom, sep="\t")
        for taxid in custom_taxon["parent_taxid"]:
            if taxid not in taxon["taxid"]:
                raise KeyError(f"Parent {taxid} from custom set doesn't exist")
        taxon = pd.concat([taxon, custom_taxon])

    taxon.to_csv(args.out, sep="\t", index=False)
    return


if __name__ == "__main__":
    main()
