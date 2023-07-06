#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.0.1"
__date__ = "07/06/2023"

"""Class PFSRun defines the basic file listing comparison behaviour.
It takes a PFSParams object and performs the comparison.
"""

# standard imports
import sys

# local imports
import pfslib.pfsout as pfsout
import pfslib.pfsoutsqlite as pfsoutsqlite
import pfslib.pfsql as pfsql

# import time


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

    def getCountFiles(self, doc="Return the number of files found"):
        return self._countFiles

    CountFiles = property(getCountFiles)

    def Run(self):
        """Run the file database comparison."""
        # startTime = time.time()

        self.createpfsout()

        self._countFiles = 0
        self._differingFileCount = 0

        try:
            self._sourceDB = self.openFileListDB(self._params.SourceDB)
            self._targetDB = self.openFileListDB(self._params.TargetDB)

            if self._targetDB is None and self._sourceDB is None:
                raise PFSRunException("No database opened!?")

            commonColNames = self.getCommonColNames()
            
            # match files source vs. target
            fileMatchStatus = self.matchFiles()
                    
            if commonColNames == []:
                return
            
            self.compareFiles(commonColNames, fileMatchStatus)
        finally:
            # close database connections
            if self._targetDB is not None:
                self.closeFileListDB(self._targetDB)
            if self._sourceDB is not None:
                self.closeFileListDB(self._sourceDB)
            # close outfile
            self._pfsout.close()

            if self._params.ShowDots:
                if self._countFiles < self._params.FilesPerDot:
                    print(".", end="")
                print("")

            if self._countFiles == 0:
                print("Databases contain no file data.")
            else:
                print(f"Compared {self._countFiles} file(s).")
                if self._differingFileCount > 0:
                    print(f"Found {self._differingFileCount} differing files.")
                else:
                    print("No differences.")
            # print("Took {0:.2f} seconds.".format(time.time() - startTime))

    def formatListStrings(self, dataList):
        """Return all elements in the list formatted as strings."""
        return [str(e) for e in dataList]

    def formatListDatabase(self, dataList):
        """Return all elements in the list converted to types suitable for database."""
        return dataList

    def createpfsout(self):
        """Create the output object for data display or storage."""
        if self._params.UseStdOut:
            self._pfsout = pfsout.PFSOutStd()
            self._formatMatchList = self.formatListStrings
        else:
            print("Write results to {}".format(self._params.OutFilePath))
            if self._params.OutFileType == 1:
                self._pfsout = pfsoutsqlite.PFSOutSqlite(
                    self._params.OutFilePath,
                    self._columns,
                )
                self._formatMatchList = self.formatListDatabase
            else:
                self._pfsout = pfsout.PFSOutCSV(
                    self._params.OutFilePath,
                    self._columns,
                )
                self._formatMatchList = self.formatListStrings

            if self._params.OutExistsMode == "":
                if self._params.OutFilePath.exists():
                    inputres = input("Output file already exists. Overwrite (Y/n)?")
                    if inputres != "" and inputres != "Y" and inputres != "y":
                        sys.exit(0)
                overwrite = "w"
            else:
                overwrite = self._params.OutExistsMode

            self._pfsout.openout(overwrite)

    def openFileListDB(self, dbfilename):
        db = pfsql.opendb(dbfilename)
        if not pfsql.tableexists(db, "filelist") or not pfsql.tableexists(db, "dirlist"):
            pfsql.closedb(db)
            raise PFSRunException(
                f"'{dbfilename}' is not a valid file listing database!"
            )
        return db

    def getCommonColNames(self):
        cntSourceCols = pfsql.gettablecolnum(self._sourceDB, "filelist")
        cntTargetCols = pfsql.gettablecolnum(self._targetDB, "filelist")
        colNamesSource = pfsql.gettablecolnames(self._sourceDB, "filelist")
        colNamesTarget = pfsql.gettablecolnames(self._targetDB, "filelist")
        
        if cntSourceCols > 2 and cntTargetCols > 2:
            return [c for c in colNamesSource if c in colNamesTarget]
        else:
            return []

    def matchFiles(self):
        fileMatchStatus = {}
        joinQuery = (
            "SELECT dirlist.path, filelist.path, filelist.filename FROM dirlist"
            + " INNER JOIN filelist ON dirlist.id = filelist.path"
        )
        matchQuery = (
            joinQuery + " WHERE dirlist.path = ? AND filelist.filename = ?"
        )
        for sourcerow in self._sourceDB[1].execute(joinQuery):
            self._countFiles += 1
            filePath = "\\".join((sourcerow[0], sourcerow[2]))
            print(filePath, end="")

            res = self._targetDB[1].execute(
                matchQuery,
                (
                    sourcerow[0], sourcerow[2],
                ),
            )
            matchRow = res.fetchone()
            if matchRow is None:
                fileMatchStatus[filePath] = 1
                print(" ...no match! [on target]")
                continue
            else:
                print("")

            fileMatchStatus[filePath] = 0
            # print(matchRow)
            
        # match files target vs. source
        for targetrow in self._targetDB[1].execute(joinQuery):
            filePath = "\\".join((targetrow[0], targetrow[2]))
            if filePath not in fileMatchStatus:
                self._countFiles += 1
                fileMatchStatus[filePath] = 2
                print(filePath, " ...no match! [on source]")
                
        return fileMatchStatus

    def compareFiles(self, commonColNames, fileMatchStatus):
        # resulting row pattern: dirlist.path, dirlist.id = filelist.path, filelist.filename...
        filelistCols = (", ".join(("filelist."+c) for c in commonColNames)).strip(", ")
        joinQuery = (
            f"SELECT dirlist.path, {filelistCols} FROM dirlist"
            + " INNER JOIN filelist ON dirlist.id = filelist.path"
        )
        matchQuery = (
            joinQuery + " WHERE dirlist.path = ? AND filelist.filename = ?"
        )
        # compare matching file's properties (existing in both DBs)
        for files in fileMatchStatus.items():
            if files[1] == 0:
                lastslash = files[0].rindex("\\")
                (path, filename) = (files[0][:lastslash], files[0][lastslash+1:])
                res = self._sourceDB[1].execute(matchQuery, (path, filename))
                sourceRow = res.fetchone()
                res = self._targetDB[1].execute(matchQuery, (path, filename))
                targetRow = res.fetchone()
                
                firstDifference = True
                for i in range(3, len(commonColNames) + 1):
                    if not sourceRow[i] == targetRow[i]:
                        if firstDifference:
                            print(f"{files[0]} different!")
                            firstDifference = False
                            self._differingFileCount += 1
                        print("    @" + commonColNames[i-1], sourceRow[i], "->", targetRow[i])
        
    def closeFileListDB(self, db):
        pfsql.closedb(db)
