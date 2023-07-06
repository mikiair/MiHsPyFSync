#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.0.1"
__date__ = "07/06/2023"

"""Class PFSParams defines a set of parameters used for comparing file lists:
source and target database files, a file for result output,
and additional attribute for stdout usage.
"""

# standard imports
import pathlib


class PFSParams:
    """Class PFSParams defines a set of parameters used for searching files:
    a match pattern, a directory to scan, option to recurse into sub-folders,
    and a file for result output, and additional attribute for stdout usage.
    """

    def __init__(self, sourcedb, targetdb, outfile, outexistsmode, nodots, dots):
        self._SourceDB = sourcedb
        self._TargetDB = targetdb
        self._OutFile = outfile
        self._UseStdOut = outfile is None
        if not self._UseStdOut:
            self._OutFileType = 0 if str(outfile).lower().endswith(".csv") else 1
        else:
            self._OutFileType = None
        self._OutExistsMode = outexistsmode
        self._ShowDots = not self._UseStdOut and not nodots
        if self._ShowDots:
            self._FilesPerDot = pow(10, dots)
        else:
            self._FilesPerDot = 0
        self.IsValid()

    def getSourceDB(self):
        return self._SourceDB

    SourceDB = property(getSourceDB)

    def getTargetDB(self):
        return self._TargetDB

    TargetDB = property(getTargetDB)

    def getOutFilePath(self, doc="Determines the filename of the output file"):
        return self._OutFilePath

    OutFilePath = property(getOutFilePath)

    def getUseStdOut(self, doc="If true, use stdout to print results"):
        return self._UseStdOut

    UseStdOut = property(getUseStdOut)

    def getOutFileType(self, doc="Return the file type used for output"):
        return self._OutFileType

    OutFileType = property(getOutFileType)

    def getOutExistsMode(
        self, doc="Defines the way an existing outfile will be handled"
    ):
        return self._OutExistsMode

    OutExistsMode = property(getOutExistsMode)

    def getShowDots(
        self,
        doc="If true, stdout will display a dot for each matching file (when writing to file)",
    ):
        return self._ShowDots

    ShowDots = property(getShowDots)

    def getFilesPerDot(
        self,
        doc="Return number of files to scan until a dot is printed",
    ):
        return self._FilesPerDot

    FilesPerDot = property(getFilesPerDot)

    def IsValid(self):
        if not self._SourceDB.exists():
            raise FileNotFoundError(
                "Source database '{0}' does not exist!".format(self._SourceDB)
            )
        if not self._SourceDB.is_file():
            raise IsADirectoryError("'{0}' is not a file!".format(self._SourceDB))

        if not self._TargetDB.exists():
            raise FileNotFoundError(
                "Target database '{0}' does not exist!".format(self._TargetDB)
            )
        if not self._TargetDB.is_file():
            raise IsADirectoryError("'{0}' is not a file!".format(self._TargetDB))

        return True
