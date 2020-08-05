#!/usr/bin/env python
"""
Creates photometry table
"""
# local files
from web_base import load_db, target_exists, target_argparse

# global vars
KNOWN_MAGS = {'WISE_W1': 3.368, 'WISE_W2': 4.618, 'WISE_W3': 12.082, 'WISE_W4': 22.194}  # central wave in microns


def photometry_exists(inventory: dict):
    """
    Unpacks the photometry for an object

    Parameters
    ----------
    inventory
        Dictionary of the database output for that object
    """
    try:
        if 'Photometry' not in inventory.keys():
            raise NotImplementedError('No "Photometry" key yet in db')
        elif type(inventory['Photometry']) != list:
            raise TypeError('Photometry entry needs to produce list')
    finally:
        photo = inventory['Photometry']
    # photo = [band for band in photo if band['band'] in KNOWN_MAGS.keys()]
    bands = [band['band'] for band in photo]
    mags = [band['magnitude'] for band in photo]
    errs = [band['magnitude_error'] for band in photo]
    return bands, mags, errs


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
    bands, mags, errs = photometry_exists(inventory)
    return bands, mags, errs


if __name__ == '__main__':
    _tgt = target_argparse()
    main(_tgt)
