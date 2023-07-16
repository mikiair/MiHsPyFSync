#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.2.0"
__date__ = "07/16/2023"

"""Classes in pfsout handle the output to stdout or to a CSV writer file
"""

# standard imports
import csv


class PFSOut:
    """Abstract base class for result output."""

    def __init__(self, commonColNames):
        self._commonColNames = commonColNames

    def openout(self, mode):
        pass

    def writeMatch(self, filePath, fileName, matchStatus):
        pass

    def flushMatches(self):
        pass

    def writeCompare(self, filePath, fileName, differences, sourceRow, targetRow):
        pass

    def flushCompares(self):
        pass

    def close(self):
        pass


class PFSOutStd(PFSOut):
    """Class for result output to stdout."""

    def __init__(self, commonColNames):
        super().__init__(commonColNames)
        self._currentFolder = None

    def writeMatch(self, filePath, fileName, matchStatus):
        """Print file data to stdout. If file is from next folder first print the
        folder name in a separate line.
        """
        if not filePath == self._currentFolder:
            self._currentFolder = filePath
            print(self._currentFolder + "\\")

        print("\t" + fileName, end="")

        if matchStatus == 1:
            print(" ...lonely!")
        elif matchStatus == 2:
            print(" ...extra!")
        else:
            print("")

    def writeCompare(self, filePath, fileName, differences, sourceRow, targetRow):
        if not filePath == self._currentFolder:
            self._currentFolder = filePath
            print(self._currentFolder + "\\")

        if len(differences) == 0:
            print(f"\t{fileName} ...same")
            return

        print(f"\t{fileName} ...different:")
        for d in differences:
            if not self._commonColNames[d].find("time") == -1:
                maxLen = 19
            else:
                maxLen = 16

            srFormatted = str(sourceRow[d + 1])
            srFormatted = srFormatted[: min(maxLen, len(srFormatted))].ljust(19, ".")
            trFormatted = str(targetRow[d + 1])
            trFormatted = trFormatted[: min(maxLen, len(trFormatted))].ljust(19, ".")

            print(
                "\t\t@{0}\t{1} -> {2}".format(
                    self._commonColNames[d], srFormatted, trFormatted
                )
            )


class PFSOutFile(PFSOut):
    """Class for result output to a file."""

    def __init__(self, filePath, commonColNames):
        super().__init__(commonColNames)
        self._filePath = filePath


class PFSOutCSV(PFSOutFile):
    """Class for result output to CSV file."""

    def __init__(self, filePath, commonColNames):
        super().__init__(filePath, commonColNames)
        self._outFile = None

    def openout(self, mode):
        self._outFile = open(self._filePath, mode, newline="")
        self._csvWriter = csv.writer(self._outFile, dialect="excel-tab", delimiter=";")
        columnHeader = ["path", "filename", "match"]
        if len(self._commonColNames) > 0:
            columnHeader.extend(self._commonColNames[2:])
        self._csvWriter.writerow(columnHeader)

    def writeMatch(self, filePath, fileName, matchStatus):
        """Write result data as a new line into CSV file."""
        try:
            self._csvWriter.writerow([filePath, fileName, matchStatus])
        except (Exception):
            # handle invalid chars or invalidly encoded chars
            self._csvWriter.writerow(["Error in output encoding!"])
        self._outFile.flush()

    def writeCompare(self, filePath, fileName, differences, sourceRow, targetRow):
        try:
            rowItems = [filePath, fileName, 0]
            for i in range(3, len(sourceRow)):
                if (i - 1) in differences:
                    rowItems.append("1" if sourceRow[i] > targetRow[i] else "-1")
                else:
                    rowItems.append("0")
            self._csvWriter.writerow(rowItems)
        except (Exception):
            # handle invalid chars or invalidly encoded chars
            self._csvWriter.writerow(["Error in output encoding!"])
        self._outFile.flush()

    def close(self):
        if self._outFile is not None:
            self._outFile.close()
