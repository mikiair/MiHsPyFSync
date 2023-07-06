#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.0.1"
__date__ = "07/06/2023"

"""Class handles output of file comparison results to SQLite database.
"""

# standard imports

# local imports
import pfslib.pfsout as pfsout
import pfslib.pfsql as pfsql


class PFSOutSqlite(pfsout.PFSOutFile):
    """Class handles output of matching file search results to SQLite database."""

    def __init__(self, filePath, columnNames):
        super().__init__(filePath, columnNames)
        self._qmarks = (len(self._columnNames) * "?, ").strip(", ")
        self._insertCmd = f"INSERT INTO filecomp VALUES ({self._qmarks})"

    def openout(self, mode):
        self._db = pfsql.opendb(self._filePath)

        if mode == "w":
            try:
                pfsql.droptable(self._db, "filecomp")
            except Exception:
                pass

        pfsql.createtable(self._db, "filecomp", self._columnNames)
        self._dataSets = []

    def writeMatch(self, formattedList):
        if len(self._dataSets) < 50:
            self._dataSets.append(formattedList)
        else:
            self.executeInsert()

    def close(self):
        if len(self._dataSets) > 0:
            self.executeInsert()
        pfsql.closedb(self._db)

    def executeInsert(self):
        try:
            self._db[1].executemany(self._insertCmd, self._dataSets)
            self._db[0].commit()
        except Exception as e:
            # ignore invalid data
            print(e)
            # pass
        self._dataSets = []
