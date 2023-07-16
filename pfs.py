#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.2.0"
__date__ = "07/16/2023"

"""Compare file information saved in two Sqlite databases.
"""

# local imports
import pfslib.pfsargparse as pfsargparse
import pfslib.pfsparams as pfsparams
import pfslib.pfsrun as pfsrun

# define and collect commandline arguments
# (kept outside try-catch block to leave exception messages untouched)
parser = pfsargparse.PFSArgParse(
    description="Compare file listings stored in two Sqlite database files."
)
args = parser.parse_args()

try:
    # create parameter object
    params = pfsparams.PFSParams(args)

    print("Match and compare databases...")

    run = pfsrun.PFSRun(params)

    run.Run()
except (FileNotFoundError) as e:
    print(f"File not found: {e.args[0]}")
except (IsADirectoryError) as e:
    print(f"Directory error: {e.args[0]}")
except (pfsrun.PFSRunException) as e:
    print(f"Run error: {e.args[0]}")
except (KeyboardInterrupt):
    print("Cancelled by user!")
except (Exception) as e:
    print(f"Unhandled error: {e.args[0]}")
