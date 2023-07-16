#!/usr/bin/env python

__author__ = "Michael Heise"
__copyright__ = "Copyright (C) 2023 by Michael Heise"
__license__ = "LGPL"
__version__ = "0.2.0"
__date__ = "07/16/2023"

"""Module with SQLite helper functions (copied back from MiHsPyFList/pfllib).
"""

# standard imports
import sqlite3


def opendb(dbFileName):
    """Open a SQLite database file and return a tuple with connection and cursor object."""
    connection = sqlite3.connect(dbFileName)
    cursor = connection.cursor()
    return (connection, cursor)


def tableexists(db, tableName):
    """Return true if tablename exists in the database."""
    selectCmd = (
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}'"
    )
    return db[1].execute(selectCmd) is not None


def createtable(db, newTable, columnNames, ifnotexists=False, constraint=None):
    """Create a new table with specified columns for a database referenced by a
    tuple db = (connection, cursor), optionally create if not yet existing.
    """
    definition = (", ".join(columnNames)).strip(", ")
    if constraint is not None:
        definition += ", " + constraint
    if not ifnotexists:
        sqlCmd = f"CREATE TABLE {newTable}({definition})"
    else:
        sqlCmd = f"CREATE TABLE IF NOT EXISTS {newTable}({definition})"
    db[1].execute(sqlCmd)
    db[0].commit()


def droptable(db, tableName, ifexists=False):
    """Delete a table from the database referenced by the tuple db = (connection, cursor)."""
    if ifexists:
        sqlCmd = f"DROP TABLE IF EXISTS {tableName}"
    else:
        sqlCmd = f"DROP TABLE {tableName}"
    db[1].execute(sqlCmd)
    db[0].commit()


def gettablecolnum(db, tablename):
    """Return the number of columns in the table."""
    selectCmd = f"SELECT COUNT(*) FROM pragma_table_info('{tablename}')"
    return db[1].execute(selectCmd).fetchone()[0]


def gettablecolnames(db, tablename):
    """Return a list with column names of that table."""
    selectCmd = f"SELECT name FROM pragma_table_info('{tablename}')"
    res = db[1].execute(selectCmd).fetchall()
    return [colname for colname, in res]


def getrowid(db, tablename, conditions):
    """Return the ID column value of the row matching the conditions clause or
    return None if no matching row exists.
    """
    try:
        selectCmd = f"SELECT id FROM {tablename} WHERE {conditions}"
        res = db[1].execute(selectCmd)
        return res.fetchone()
    except Exception:
        return None


def insertrow(db, tablename, parampattern, params, ifnotexists=False):
    """Insert a new row into a table with the given values."""
    if ifnotexists:
        ignore = "OR IGNORE "
    else:
        ignore = ""
    insertCmd = f"INSERT {ignore}INTO {tablename} VALUES ({parampattern})"
    db[1].execute(insertCmd, params)
    db[0].commit()


def insertidrow(db, tablename, parampattern, params):
    """Insert a row into a table with the given values and return the last row ID."""
    insertrow(db, tablename, parampattern, params)
    return db[1].lastrowid


def updaterow(db, tablename, columnpattern, condition, params):
    """Update columns in an existing row identified by a condition clause
    with new values.
    """
    if condition is not None:
        updateCmd = f"UPDATE {tablename} SET {columnpattern} WHERE {condition}"
    else:
        updateCmd = f"UPDATE {tablename} SET {columnpattern}"
    db[1].execute(updateCmd, params)
    db[0].commit()


def closedb(db):
    """Close the database connection."""
    db[0].close()
