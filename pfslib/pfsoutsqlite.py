#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.2.0"
__date__ = "07/16/2023"

"""Class handles output of file comparison results to SQLite database.
"""

# standard imports
from datetime import datetime

# local imports
import pfslib.pfsout as pfsout
import pfslib.pfsql as pfsql


class PFSOutSqlite(pfsout.PFSOutFile):
    """Class handles output of matching file search results to SQLite database."""

    def __init__(self, filePath, commonColNames):
        super().__init__(filePath, commonColNames)
        self._insertMatchCmd = (
            "INSERT INTO filecomp (path, filename, match) VALUES (?, ?, ?)"
        )
        if len(commonColNames) > 0:
            self._qmarks = ((len(self._commonColNames) + 1) * "?, ").strip(", ")
            self._insertCompareCmd = f"INSERT INTO filecomp VALUES ({self._qmarks})"

    def openout(self, mode):
        self._db = pfsql.opendb(self._filePath)

        if mode == "w":
            self.droptables()

        self.setuptables()

        self._dataSets = []

    def droptables(self):
        try:
            pfsql.droptable(self._db, "stats", True)
            pfsql.droptable(self._db, "filecomp", True)
        except Exception:
            print("Error while clearing existing data tables (check recommended)!?")

    def setuptables(self):
        columnHeader = ["path", "filename", "match INTEGER"]
        if len(self._commonColNames) > 0:
            columnsWithType = [c + " INTEGER" for c in self._commonColNames[2:]]
            columnHeader.extend(columnsWithType)

        pfsql.createtable(self._db, "filecomp", columnHeader)

    def writeStats(self, params):
        """Create statistics table if not existing and append a new row."""
        pfsql.createtable(
            self._db,
            "stats",
            [
                "id INTEGER PRIMARY KEY",
                "timestamp",
                "source",
                "target",
                "nfiles",
                "ncommon",
                "nlonely",
                "nextra",
                "nsame",
                "ndifferent",
                "duration",
            ],
            True,
        )
        self._statrowID = pfsql.insertidrow(
            self._db,
            "stats",
            (11 * "?, ").strip(", "),
            (
                None,
                datetime.now(),
                str(params.SourceDB),
                str(params.TargetDB),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        )

    def writeMatch(self, filePath, fileName, matchStatus):
        if len(self._dataSets) < 50:
            self._dataSets.append(
                (
                    filePath,
                    fileName,
                    matchStatus,
                )
            )
        else:
            self.executeInsertMatches()

    def flushMatches(self):
        """Write remaining match datasets to database."""
        if len(self._dataSets) > 0:
            self.executeInsertMatches()

    def writeCompare(self, filePath, fileName, differences, sourceRow, targetRow):
        if len(self._dataSets) < 50:
            rowItems = [filePath, fileName, 0]
            for i in range(3, len(sourceRow)):
                if (i - 1) in differences:
                    rowItems.append(1 if sourceRow[i] > targetRow[i] else -1)
                else:
                    rowItems.append(0)
            self._dataSets.append(rowItems)
        else:
            self.executeInsertCompares()

    def flushCompares(self):
        """Write remaining compare datasets to database."""
        if len(self._dataSets) > 0:
            self.executeInsertCompares()

    def updateStats(self, resultStats, duration):
        if len(resultStats) < 5:
            columnPattern = (
                "=?, ".join(["nfiles", "ncommon", "nlonely", "nextra", "duration"])
                + "=?"
            )
            nfiles, ncommon, nlonely, nextra = resultStats
            params = (nfiles, ncommon, nlonely, nextra, duration)
        else:
            columnPattern = (
                "=?, ".join(
                    [
                        "nfiles",
                        "ncommon",
                        "nlonely",
                        "nextra",
                        "nsame",
                        "ndifferent",
                        "duration",
                    ]
                )
                + "=?"
            )
            nfiles, ncommon, nlonely, nextra, nsame, ndifferent = resultStats
            params = (nfiles, ncommon, nlonely, nextra, nsame, ndifferent, duration)

        pfsql.updaterow(
            self._db,
            "stats",
            columnPattern,
            f"ID={self._statrowID}",
            params,
        )

    def close(self):
        """Close database connection."""
        pfsql.closedb(self._db)

    def executeInsertMatches(self):
        self.executeInsert(self._insertMatchCmd)

    def executeInsertCompares(self):
        self.executeInsert(self._insertCompareCmd)

    def executeInsert(self, cmd):
        try:
            self._db[1].executemany(cmd, self._dataSets)
            self._db[0].commit()
        except Exception as e:
            # ignore invalid data
            print(e)
            # pass
        self._dataSets = []
