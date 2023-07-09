#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.1.0"
__date__ = "07/09/2023"
"""Class handles output of file comparison results to SQLite database.
"""

# standard imports

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
            try:
                pfsql.droptable(self._db, "filecomp")
            except Exception:
                pass

        columnHeader = ["path", "filename", "match INTEGER"]
        if len(self._commonColNames) > 0:
            columnsWithType = [c+" INTEGER" for c in self._commonColNames[2:]]
            columnHeader.extend(columnsWithType)

        pfsql.createtable(self._db, "filecomp", columnHeader)

        self._dataSets = []

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
