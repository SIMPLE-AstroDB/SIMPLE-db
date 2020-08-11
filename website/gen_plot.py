#!/usr/bin/env python
"""
Generates the script and div for the bokeh plot of spectra

Methods
-------
spectra_exists
    Takes spectra from database for given object
main
    Queries database for given object and finds spectra
"""
# local files
from web_base import load_db, target_exists, target_argparse


def spectra_exists(inventory: dict):
    """
    Unpacks the spectra from the object inventory

    Parameters
    ----------
    inventory
        Dictionary of the database output for that object
    Returns
    -------
    _
        The spectra
    """
    try:
        if 'Spectra' not in inventory.keys():
            raise NotImplementedError('No "Spectra" key yet in db')
    except NotImplementedError:
        pass  # for now
    # TODO: when implemented add the unpacking and handling of spectra here (astropy specutils?)
    # TODO: including multiple spectra per object
    return


def main(target: str):
    """
    Main module

    Parameters
    ----------
    target
        Name of the target to be searched for, given as script argument
    """
    db = load_db()  # load simple database
    inventory = target_exists(db, target)  # query database for given object
    spectra = spectra_exists(inventory)  # pull spectra from database query
    return spectra


if __name__ == '__main__' or 'bokeh' in __name__:
    _tgt = target_argparse()  # pull target name from system arguments
    main(_tgt)
