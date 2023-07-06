#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.0.1"
__date__ = "07/06/2023"

"""Module with SQL helper functions.
"""

# standard imports
import sqlite3


def opendb(dbFileName):
    """Open a database file and return a tuple with connection and cursor object."""
    connection = sqlite3.connect(dbFileName)
    cursor = connection.cursor()
    return (connection, cursor)


def tableexists(db, tableName):
    """Return true if tablename exists in the database."""
    selectCmd = (
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}'"
    )
    return db[1].execute(selectCmd) is not None


def createtable(db, newTable, columnNames):
    """Create a new table with specified columns for a database referenced by a
    tuple db = (connection, cursor).
    """
    joinedCols = (", ".join(columnNames)).strip(", ")
    sqlCmd = f"CREATE TABLE {newTable}({joinedCols})"
    db[1].execute(sqlCmd)
    db[0].commit()


def droptable(db, tableName):
    """Delete a table from the database reference by the tuple db = (connection, cursor)."""
    sqlCmd = f"DROP TABLE {tableName}"
    db[1].execute(sqlCmd)
    db[0].commit()


def gettablecolnum(db, tablename):
    """Return the number of columns in the table."""
    selectCmd = (
        f"SELECT COUNT(*) FROM pragma_table_info('{tablename}')"
    )
    return db[1].execute(selectCmd).fetchone()[0]


def gettablecolnames(db, tablename):
    """Return a list with column names of that table."""
    selectCmd = (
        f"SELECT name FROM pragma_table_info('{tablename}')"
    )
    res = db[1].execute(selectCmd).fetchall()
    return [colname for colname, in res]
    
    
def closedb(db):
    """Close the database connection."""
    db[0].close()
