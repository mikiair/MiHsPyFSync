#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.0.1"
__date__ = "07/06/2023"

"""Classes in PFSArgParse derive from ArgumentParser and define different argument
parsers with a set of default arguments for file listing comparison.
"""

# standard imports
import pathlib
from argparse import ArgumentParser


class PFSArgParse(ArgumentParser):
    def __init__(self, description):
        super().__init__(description)

        self.add_argument(
            "source",
            type=pathlib.Path,
            help="database file on source",
        )
        self.add_argument(
            "target",
            type=pathlib.Path,
            help="database file on target",
        )
        self.add_argument(
            "outfile",
            nargs="?",
            type=pathlib.Path,
            default=None,
            help="CSV or database file to write results to [default=stdout]",
        )

        fileopt_group = self.add_argument_group(
            "file options",
            "optional arguments apply when writing to CSV or database file"
            + " (ignored otherwise)",
        )

        existmode_group = fileopt_group.add_mutually_exclusive_group()

        existmode_group.add_argument(
            "-o",
            "--overwrite",
            dest="overwrite",
            action="store_const",
            const="w",
            default="",
            help="overwrite the outfile if existent",
        )
        existmode_group.add_argument(
            "-a",
            "--append",
            dest="append",
            action="store_const",
            const="a",
            default="",
            help="append to the outfile if existent",
        )

        dotmode_group = fileopt_group.add_mutually_exclusive_group()

        dotmode_group.add_argument(
            "-n",
            "--nodots",
            dest="nodots",
            action="store_true",
            default=False,
            help="do not display dots for matches",
        )
        dotmode_group.add_argument(
            "-d",
            "--dots",
            dest="dots",
            type=int,
            default=0,
            help="logarithmic number of matching files to display one dot for\n"
            + "(i.e. 0=every file, 1=each 10 files, 2=each 100 files...)",
        )
