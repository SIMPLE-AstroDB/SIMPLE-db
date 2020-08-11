#!/usr/bin/env python
"""
Prints the info about the given object in this div

Methods
-------
output
    Converts inventory into JSON string
main
    Loads simple database, queries for a target and pulls out inventory
"""
# inbuilt libs
import json

# local libs
from web_base import load_db, target_exists, target_argparse


def output(inventory: dict):
    """
    Converts object inventory to json string

    Parameters
    ----------
    inventory
        The dictionary of data about given object

    Returns
    -------
    inventory
        The json string of data about given object

    """
    if 'Photometry' in inventory.keys():
        del inventory['Photometry']  # will not print photometry data raw to page
    inventory = json.dumps(inventory, indent=4)  # convert to json string and pretty print
    return inventory


def main(target: str):
    """
    Main module, loading simple database, querying and outputting object inventory as json string

    Parameters
    ----------
    target
        The name of the object to query database by

    Returns
    -------
    inventory
        The json string of the inventory for that object
    """
    db = load_db()  # load database
    inventory = target_exists(db, target)  # query database for given object
    inventory = output(inventory)  # converts inventory of object to string
    return inventory


if __name__ == '__main__' or 'bokeh' in __name__:
    _tgt = target_argparse()  # if running from command line
    main(_tgt)
