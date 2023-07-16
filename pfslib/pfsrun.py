#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.2.0"
__date__ = "07/16/2023"

"""Class PFSRun defines the basic file listing comparison behaviour.
It takes a PFSParams object and performs the comparison.
"""

# standard imports
import sys
import time

# local imports
import pfslib.pfsout as pfsout
import pfslib.pfsoutsqlite as pfsoutsqlite
import pfslib.pfsql as pfsql


class PFSRunException(Exception):
    """Exception class used by PFSRun."""


class PFSRun:
    """Class PFSRun defines the basic file listing comparison behaviour.
    It takes a PFSParams object and performs the comparison.
    """

    def __init__(self, params):
        self._params = params
        self._countFiles = 0
        self._sourceDB = None
        self._targetDB = None
        self._pfsout = None

    def getCountFiles(self, doc="Return the number of files found"):
        return self._countFiles

    CountFiles = property(getCountFiles)

    def Run(self):
        """Run the file database comparison."""
        resultStats = None

        self._countFiles = 0
        self._differingFileCount = 0

        try:
            self._sourceDB = self.openFileListDB(self._params.SourceDB)
            self._targetDB = self.openFileListDB(self._params.TargetDB)

            if self._targetDB is None and self._sourceDB is None:
                raise PFSRunException("No database opened!?")

            self._doCompare = self.getCommonColNames()
            
            # call after source/target database were opened
            # and common column names were set
            self.createpfsout()
            
            # set start time after possible user interaction in createpflout
            startTime = time.time()

            try:
                # match files source vs. target
                fileMatchStatus = self.matchFiles()

                if self._countFiles == 0:
                    print("Databases contain no file data.")
                    return

                # compare properties of files found in both databases
                if self._doCompare:
                    self.compareFiles(fileMatchStatus)

                resultStats = self.getResultStats(fileMatchStatus)
                self.printResults(resultStats)
            finally:
                duration = time.time() - startTime

                # close outfile
                if self._params.OutFileType == 1 and resultStats is not None:
                    self._pfsout.updateStats(resultStats, duration)
                self._pfsout.close()
                
                print("Took {0:.2f} seconds.".format(duration))
        finally:
            # close database connections
            if self._targetDB is not None:
                self.closeFileListDB(self._targetDB)
            if self._sourceDB is not None:
                self.closeFileListDB(self._sourceDB)

    def openFileListDB(self, dbfilename):
        db = pfsql.opendb(dbfilename)
        if not pfsql.tableexists(db, "filelist") or not pfsql.tableexists(
            db, "dirlist"
        ):
            pfsql.closedb(db)
            raise PFSRunException(
                f"'{dbfilename}' is not a valid file listing database!"
            )
        return db

    def getCommonColNames(self):
        """Get a list of column names present in both 'filelist' tables
        in the two databases compared. Return true if this list is not empty.
        """
        cntSourceCols = pfsql.gettablecolnum(self._sourceDB, "filelist")
        cntTargetCols = pfsql.gettablecolnum(self._targetDB, "filelist")
        colNamesSource = pfsql.gettablecolnames(self._sourceDB, "filelist")
        colNamesTarget = pfsql.gettablecolnames(self._targetDB, "filelist")

        if cntSourceCols > 2 and cntTargetCols > 2:
            self._commonColNames = [
                c for c in colNamesSource if not c == "id" and c in colNamesTarget
            ]
            if not self._params.CompareCTime:
                try:
                    self._commonColNames.remove("ctime")
                except Exception:
                    pass
            return not self._commonColNames == []
        else:
            self._commonColNames = []
            return False

    def createpfsout(self):
        """Create the output object for data display or storage."""
        if self._params.UseStdOut:
            self._pfsout = pfsout.PFSOutStd(self._commonColNames)
            return

        print("Write results to {}".format(self._params.OutFilePath))

        if self._params.OutExistsMode == "":
            if self._params.OutFilePath.exists():
                inputres = input("Output file already exists. Overwrite (Y/n)?")
                if inputres != "" and inputres != "Y" and inputres != "y":
                    sys.exit(0)
            overwrite = "w"
        else:
            overwrite = self._params.OutExistsMode

        if self._params.OutFileType == 1:
            self._pfsout = pfsoutsqlite.PFSOutSqlite(
                self._params.OutFilePath,
                self._commonColNames,
            )
        else:
            self._pfsout = pfsout.PFSOutCSV(
                self._params.OutFilePath,
                self._commonColNames,
            )

        self._pfsout.openout(overwrite)

        if self._params.OutFileType == 1:
            self._pfsout.writeStats(self._params)

    def matchFiles(self):
        """Return a dictionary with filenames found in one or both databases,
        and assigned match status indicating file presence.
        """
        try:
            fileMatchStatus = {}
            joinQuery = (
                "SELECT dirlist.path, filelist.path, filelist.filename FROM dirlist"
                + " INNER JOIN filelist ON dirlist.id = filelist.path"
            )
            matchQuery = joinQuery + " WHERE dirlist.path = ? AND filelist.filename = ?"
            for sourcerow in self._sourceDB[1].execute(joinQuery):
                self._countFiles += 1
                filePath = "\\".join((sourcerow[0], sourcerow[2]))

                res = self._targetDB[1].execute(
                    matchQuery,
                    (
                        sourcerow[0],
                        sourcerow[2],
                    ),
                )
                matchRow = res.fetchone()
                if matchRow is None:
                    fileMatchStatus[filePath] = 1
                    self._pfsout.writeMatch(sourcerow[0], sourcerow[2], 1)
                    self.printdot()
                    continue

                fileMatchStatus[filePath] = 0
                if not self._doCompare:
                    self._pfsout.writeMatch(sourcerow[0], sourcerow[2], 0)
                self.printdot()

            # match files target vs. source to find extras
            for targetrow in self._targetDB[1].execute(joinQuery):
                filePath = "\\".join((targetrow[0], targetrow[2]))
                if filePath not in fileMatchStatus:
                    self._countFiles += 1
                    fileMatchStatus[filePath] = 2
                    self._pfsout.writeMatch(targetrow[0], targetrow[2], 2)
                    self.printdot()
        finally:
            self._pfsout.flushMatches()

        return fileMatchStatus

    def compareFiles(self, fileMatchStatus):
        try:
            # resulting row pattern:
            # dirlist.path, dirlist.id = filelist.path, filelist.filename...
            filelistCols = (
                ", ".join(("filelist." + c) for c in self._commonColNames)
            ).strip(", ")
            joinQuery = (
                f"SELECT dirlist.path, {filelistCols} FROM dirlist"
                + " INNER JOIN filelist ON dirlist.id = filelist.path"
            )
            matchQuery = joinQuery + " WHERE dirlist.path = ? AND filelist.filename = ?"

            # compare matching file's properties (existing in both DBs)
            for files in fileMatchStatus.items():
                if files[1] == 0:
                    lastslash = files[0].rindex("\\")
                    (path, filename) = (files[0][:lastslash], files[0][lastslash + 1 :])
                    res = self._sourceDB[1].execute(matchQuery, (path, filename))
                    sourceRow = res.fetchone()
                    res = self._targetDB[1].execute(matchQuery, (path, filename))
                    targetRow = res.fetchone()

                    differences = []
                    for i in range(3, len(self._commonColNames) + 1):
                        if not sourceRow[i] == targetRow[i]:
                            differences.append(i - 1)

                    if len(differences) > 0:
                        self._differingFileCount += 1

                    self._pfsout.writeCompare(
                        path, filename, differences, sourceRow, targetRow
                    )
        finally:
            self._pfsout.flushCompares()

    def getResultStats(self, fileMatchStatus):
        commonFileNum = sum(1 for m in fileMatchStatus.values() if m == 0)
        lonelyFileNum = sum(1 for m in fileMatchStatus.values() if m == 1)
        extraFileNum = sum(1 for m in fileMatchStatus.values() if m == 2)

        if not self._doCompare:
            return (len(fileMatchStatus), commonFileNum, lonelyFileNum, extraFileNum)

        return (
            len(fileMatchStatus),
            commonFileNum,
            lonelyFileNum,
            extraFileNum,
            commonFileNum - self._differingFileCount,
            self._differingFileCount,
        )

    def printResults(self, resultStats):
        if self._params.ShowDots:
            if self._countFiles < self._params.FilesPerDot:
                print(".", end="")

        print("")
        print("Match results:")
        print("\t# of files:  {0:5}".format(resultStats[0]))
        print(
            "\t# of common: {0:5}\t# of lonely:    {1:5}\t# of extra:     {2:5}".format(
                resultStats[1],
                resultStats[2],
                resultStats[3],
            )
        )
        if len(resultStats) == 6:
            print(
                "\t# of same:   {0:5}\t# of different: {1:5}".format(
                    resultStats[4], resultStats[5]
                )
            )

    def closeFileListDB(self, db):
        pfsql.closedb(db)

    def printdot(self):
        if self._params.ShowDots and self._countFiles % self._params.FilesPerDot == 0:
            print(".", end="", flush=True)
