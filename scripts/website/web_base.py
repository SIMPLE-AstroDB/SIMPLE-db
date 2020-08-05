#!/usr/bin/env python
"""
Base functions for web site python scripts
"""
# inbuilt libs
import argparse

# 3rd party libs
from astrodbkit2.astrodb import Database

# global vars
CONNECTION_STRING = 'sqlite:///website/SIMPLE.db'


def load_db() -> Database:
    """
    Loads the database and creates it if it wasn't there already

    Returns
    -------
    _
        The database object
    """
    try:
        db = Database(CONNECTION_STRING)
    except RuntimeError:  # db not created
        from astrodbkit2.astrodb import create_database
        create_database(CONNECTION_STRING)
        db = Database(CONNECTION_STRING)
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
    inventory = db.inventory(target)
    if inventory == {}:  # empty dict, target not in db
        raise KeyError(f'Target {target} not in db')
    return inventory


def target_argparse():
    myargs = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    myargs.add_argument('-t', '--target-name', required=True, type=str, help='Name of the target')
    args = myargs.parse_args()
    _tgt = args.target_name
    return _tgt


def main():
    return


if __name__ == '__main__':
    main()
