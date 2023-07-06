#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.0.1"
__date__ = "07/02/2023"

"""Classes in pfsout handle the output to stdout or to a CSV writer file
"""

# standard imports
import csv


class PFSOut:
    """Abstract base class for result output."""

    def __init__(self):
        pass

    def openout(self, mode):
        pass

    def writeResult(self, formattedList):
        pass

    def close(self):
        pass


class PFSOutStd(PFSOut):
    """Class for result output to stdout."""

    def __init__(self):
        self._currentFolder = ""

    def writeResult(self, formattedList):
        """Print file data to stdout. If file is from next folder first print the
        folder name in a separate line.
        """
        if not formattedList[0] == self._currentFolder:
            self._currentFolder = formattedList[0]
            print(self._currentFolder + ":")
        if len(formattedList) > 2:
            print("\t" + formattedList[1] + " - " + "/".join(formattedList[2:]))
        else:
            print("\t" + formattedList[1])


class PFSOutFile(PFSOut):
    """Class for result output to a file."""

    def __init__(self, filePath, columnNames):
        self._filePath = filePath
        self._columnNames = columnNames


class PFSOutCSV(PFSOutFile):
    """Class for result output to CSV file."""

    def __init__(self, filePath, columnNames):
        super().__init(filePath, columnNames)

    def openout(self, mode):
        self._outFile = open(self._filePath, mode, newline="")
        self._csvWriter = csv.writer(self._outFile, dialect="excel-tab", delimiter=";")
        self._csvWriter.writerow(self._columnNames)

    def writeResult(self, formattedList):
        """Write result data as a new line into CSV file."""
        try:
            self._csvWriter.writerow(formattedList)
        except (Exception):
            # handle invalid chars or invalidly encoded chars
            self._csvWriter.writerow(["Error in output encoding!"])
        self._outFile.flush()

    def close(self):
        self._outFile.close()
