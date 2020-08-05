#!/usr/bin/env python
"""
Generates the script and div for the bokeh plot of spectra
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
    db = load_db()
    inventory = target_exists(db, target)
    spectra = spectra_exists(inventory)
    return spectra


if __name__ == '__main__':
    _tgt = target_argparse()
    main(_tgt)
