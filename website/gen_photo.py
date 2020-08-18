#!/usr/bin/env python
"""
Creates photometry table

Attributes
----------
KNOWN_MAGS
    Dictionary of photometric bands against central wavelength in microns

Methods
-------
photometry_exists
    Grabs photometry from object inventory on query
main
    Queries database for given object and finds photometry if it exists
"""
# local files
from web_base import load_db, target_exists, target_argparse

# global vars
# TODO: expand known mags or use different method of initialising tables depending on if plotting photometry
KNOWN_MAGS = {'WISE_W1': 3.368, 'WISE_W2': 4.618, 'WISE_W3': 12.082, 'WISE_W4': 22.194}  # central wave in microns


def photometry_exists(inventory: dict):
    """
    Unpacks the photometry for an object

    Parameters
    ----------
    inventory
        Dictionary of the database output for that object
    """
    bands, mags, errs = [], [], []
    try:
        if 'Photometry' not in inventory.keys():
            raise NotImplementedError('No "Photometry" key yet in db')
        elif type(inventory['Photometry']) != list:
            raise TypeError('Photometry entry needs to produce list')
    except (NotImplementedError, TypeError):
        bands = [*KNOWN_MAGS.keys(), ]
        mags, errs = [None, ] * len(bands), [None, ] * len(bands)
        return bands, mags, errs
    else:
        photo = inventory['Photometry']
    for band in photo:
        bands.append(band['band'])  # photometric band name
        mags.append(band['magnitude'])  # magnitude for object
        errs.append(band['magnitude_error'])  # magnitude error for object
    return bands, mags, errs


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
    bands, mags, errs = photometry_exists(inventory)  # get lists for photometry
    return bands, mags, errs


if __name__ == '__main__' or 'bokeh' in __name__:
    _tgt = target_argparse()  # pull target name from system arguments
    main(_tgt)
