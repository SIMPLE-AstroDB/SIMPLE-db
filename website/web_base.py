#!/usr/bin/env python
"""
Base functions for web site python scripts, not really intended to be run directly

Attributes
----------
path
    Path to this file then shifted up one directory
CONNECTION_STRING
    The astrodbkit2 required string for opening the simple database with sqlite

Methods
-------
load_db
    Loads the simple database and returns database object
target_exists
    Checks a target exists in the database by querying by name, returns its inventory
target_argparse
    If using system arguments when calling, parses the target name
"""
# inbuilt libs
import argparse
import os

# 3rd party libs
from astrodbkit2.astrodb import Database

# global vars
path = os.path.realpath(__file__)  # path to file
path = '/'.join(path.split('/')[:-2])  # go to 1 directory up
CONNECTION_STRING = f'sqlite:///{path}/SIMPLE.db'  # should be in base directory


def load_db() -> Database:
    """
    Loads the database

    Returns
    -------
    _
        The database object
    """
    try:
        db = Database(CONNECTION_STRING)  # open database
    except RuntimeError as e:  # db not created
        raise RuntimeError(f'{e}\nDatabase not present in {path} for website initialisation')  # want it to die here
    return db


def target_exists(db: Database, target: str) -> dict:
    """
    Loads the database and the inventory for that object if it exist

    Parameters
    ----------
    db
        The simple database
    target
        The target name to be parsed by astrodbkit2

    Returns
    -------
    inventory
        Dictionary of the database output for that object
    """
    inventory = db.inventory(target)  # pull out inventory for given object
    if inventory == {}:  # empty dict, target not in db
        raise KeyError(f'Target {target} not in db')  # should not happen if querying off db in first place
    return inventory


def target_argparse() -> str:
    """
    Parses name of target if being run from script using system arguments

    Returns
    -------
    _tgt
        Name of the target to query database with
    """
    myargs = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    myargs.add_argument('-t', '--target-name', required=True, type=str, help='Name of the target')
    args = myargs.parse_args()
    _tgt = args.target_name
    return _tgt

