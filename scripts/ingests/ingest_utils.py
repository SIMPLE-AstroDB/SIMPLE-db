"""
Utils functions for use in ingests
"""
from astroquery.simbad import Simbad
from astropy.coordinates import SkyCoord
import astropy.units as u

import numpy as np
import numpy.ma as ma
import pandas as pd
from sqlalchemy import func

import dateutil
import re

from scripts.ingests.utils import *

logger = logging.getLogger('SIMPLE')


# SOURCES
def ingest_sources(db, sources, references, ras=None, decs=None, comments=None, epochs=None,
                   equinoxes=None, raise_error=True):
    """
    Script to ingest sources

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    sources: list[str]
        Names of sources
    references: str or list[strings]
        Discovery references of sources
    ras: list[floats], optional
        Right ascensions of sources. Decimal degrees.
    decs: list[floats], optional
        Declinations of sources. Decimal degrees.
    comments: list[strings], optional
        Comments
    epochs: str or list[str], optional
        Epochs of coordinates
    equinoxes: str or list[string], optional
        Equinoxes of coordinates
    raise_error: bool, optional
        True (default): Raise an error if a source cannot be ingested
        False: Log a warning but skip sources which cannot be ingested

    Returns
    -------

    None

    """
    # TODO: add example

    # SETUP INPUTS
    if ras is None and decs is None:
        coords = False
    else:
        coords = True

    n_sources = len(sources)

    # Convert single element input values into lists
    input_values = [references, epochs, equinoxes, comments]
    for i, input_value in enumerate(input_values):
        if input_value is None:
            input_values[i] = [None] * n_sources
        elif isinstance(input_value, str):
            input_values[i] = [input_value] * n_sources
    references, epochs, equinoxes, comments = input_values

    n_added = 0
    n_existing = 0
    n_names = 0
    n_alt_names = 0
    n_skipped = 0
    n_multiples = 0

    logger.info(f"Trying to add {n_sources} sources")

    # Loop over each source and decide to ingest, skip, or add alt name
    for i, source in enumerate(sources):
        # Find out if source is already in database or not
        if coords:
            name_matches = find_source_in_db(db, source, ra=ras[i], dec=decs[i])
        else:
            name_matches = find_source_in_db(db, source)

        if len(name_matches) == 1:  # Source is already in database
            n_existing += 1
            msg1 = f"{i}: Skipping {source}. Already in database. \n "
            msg2 = f"{i}: Match found for {source}: {name_matches[0]}"
            logger.debug(msg1 + msg2)

            # Figure out if ingest name is an alternate name and add
            db_matches = db.search_object(source, output_table='Sources', fuzzy_search=False)
            if len(db_matches) == 0:
                alt_names_data = [{'source': name_matches[0], 'other_name': source}]
                try:
                    db.Names.insert().execute(alt_names_data)
                    logger.debug(f"{i}: Name added to database: {alt_names_data}\n")
                    n_alt_names += 1
                except sqlalchemy.exc.IntegrityError as e:
                    msg = f"{i}: Could not add {alt_names_data} to database"
                    logger.warning(msg)
                    if raise_error:
                        raise SimpleError(msg + '\n' + str(e))
                    else:
                        continue
            continue  # Source is already in database, nothing new to ingest
        elif len(name_matches) > 1:  # Multiple source matches in the database
            n_multiples += 1
            msg1 = f"{i} Skipping {source} "
            msg = f"{i} More than one match for {source}\n {name_matches}\n"
            logger.warning(msg1 + msg)
            if raise_error:
                raise SimpleError(msg)
            else:
                continue
        elif len(name_matches) == 0:  # No match in the database, INGEST!
            if coords:  # Coordinates were provided as input
                ra = ras[i]
                dec = decs[i]
                epoch = None if ma.is_masked(epochs[i]) else epochs[i]
                equinox = None if ma.is_masked(equinoxes[i]) else equinoxes[i]
            else:  # Try to get coordinates from SIMBAD
                simbad_result_table = Simbad.query_object(source)
                if simbad_result_table is None:
                    n_skipped += 1
                    msg = f"{i}: Skipping: {source}. Coordinates are needed and could not be retrieved from SIMBAD. \n"
                    logger.warning(msg)
                    if raise_error:
                        raise SimpleError(msg)
                    else:
                        continue
                elif len(simbad_result_table) == 1:
                    simbad_coords = simbad_result_table['RA'][0] + ' ' + simbad_result_table['DEC'][0]
                    simbad_skycoord = SkyCoord(simbad_coords, unit=(u.hourangle, u.deg))
                    ra = simbad_skycoord.to_string(style='decimal').split()[0]
                    dec = simbad_skycoord.to_string(style='decimal').split()[1]
                    epoch = '2000'  # Default coordinates from SIMBAD are epoch 2000.
                    equinox = 'J2000'  # Default frame from SIMBAD is IRCS and J2000.
                    msg = f"Coordinates retrieved from SIMBAD {ra}, {dec}"
                    logger.debug(msg)
                else:
                    n_skipped += 1
                    msg = f"{i}: Skipping: {source}. Coordinates are needed and could not be retrieved from SIMBAD. \n"
                    logger.warning(msg)
                    if raise_error:
                        raise SimpleError(msg)
                    else:
                        continue

            logger.debug(f"{i}: Ingesting {source}. Not already in database. ")
        else:
            msg = f"{i}: unexpected condition encountered ingesting {source}"
            logger.error(msg)
            raise SimpleError(msg)

        # Construct data to be added
        source_data = [{'source': source,
                        'ra': ra,
                        'dec': dec,
                        'reference': references[i],
                        'epoch': epoch,
                        'equinox': equinox,
                        'comments': None if ma.is_masked(comments[i]) else comments[i]}]
        names_data = [{'source': source,
                       'other_name': source}]

        # Try to add the source to the database
        try:
            db.Sources.insert().execute(source_data)
            n_added += 1
            msg = f"Added {str(source_data)}"
            logger.debug(msg)
        except sqlalchemy.exc.IntegrityError:
            if ma.is_masked(source_data[0]['reference']):  # check if reference is blank
                msg = f"{i}: Skipping: {source}. Discovery reference is blank. \n"
                msg2 = f"\n {str(source_data)}\n"
                logger.warning(msg)
                logger.debug(msg2)
                n_skipped += 1
                if raise_error:
                    raise SimpleError(msg + msg2)
                else:
                    continue
            elif db.query(db.Publications).filter(db.Publications.c.name == references[i]).count() == 0:
                # check if reference is in Publications table
                msg = f"{i}: Skipping: {source}. Discovery reference {references[i]} is not in Publications table. \n" \
                      f"(Add it with add_publication function.) \n "
                msg2 = f"\n {str(source_data)}\n"
                logger.warning(msg)
                logger.debug(msg2)
                n_skipped += 1
                if raise_error:
                    raise SimpleError(msg + msg2)
                else:
                    continue
            # try reference without last letter e.g.Smit04 instead of Smit04a
            # elif source_data[0]['reference'][-1] in ('a', 'b'):
            #    source_data[0]['reference'] = references[i][:-1]
            #    try:
            #        db.Sources.insert().execute(source_data)
            #        n_added += 1
            #        msg = f"Added \n {str(source_data)}"
            #        logger.debug(msg)
            #    except sqlalchemy.exc.IntegrityError:
            #        msg = f"Skipping {source} "
            #        msg2 = f"\n {str(source_data)} " \
            #               f"\n Discovery reference may not exist in the Publications table. " \
            #               "(Add it with add_publication function.) \n "
            #        logger.warning(msg)
            #        logger.debug(msg2)
            #        n_skipped += 1
            #        if raise_error:
            #            raise SimpleError(msg + msg2)
            #        else:
            #            continue
            else:
                msg = f"{i}: Skipping: {source}. Not sure why."
                msg2 = f"\n {str(source_data)} "
                logger.warning(msg)
                logger.debug(msg2)
                n_skipped += 1
                if raise_error:
                    raise SimpleError(msg + msg2)
                else:
                    continue

        # Try to add the source name to the Names table
        try:
            db.Names.insert().execute(names_data)
            logger.debug(f"Name added to database: {names_data}\n")
            n_names += 1
        except sqlalchemy.exc.IntegrityError:
            msg = f"{i}: Could not add {names_data} to database"
            logger.warning(msg)
            if raise_error:
                raise SimpleError(msg)
            else:
                continue

    logger.info(f"Sources added to database: {n_added}")
    logger.info(f"Names added to database: {n_names} \n")
    logger.info(f"Sources already in database: {n_existing}")
    logger.info(f"Alt Names added to database: {n_alt_names} \n")
    logger.info(f"Sources NOT added to database because multiple matches: {n_multiples} \n")
    logger.info(f"Sources NOT added to database: {n_skipped} \n")

    if n_added != n_names:
        msg = f"Number added should equal names added."
        raise SimpleError(msg)

    if n_added + n_existing + n_multiples + n_skipped != n_sources:
        msg = f"Number added + Number skipped doesn't add up to total sources"
        raise SimpleError(msg)

    return


# SURVEY DATA
def find_survey_name_in_simbad(sources, desig_prefix, source_id_index=None):
    """
    Function to extract source designations from SIMBAD

    Parameters
    ----------
    sources: astropy.table.Table
        Sources names to search for in SIMBAD
    desig_prefix
        prefix to search for in list of identifiers
    source_id_index
        After a designation is split, this index indicates source id suffix.
        For example, source_id_index = 2 to extract suffix from "Gaia DR2" designations.
        source_id_index = 1 to exctract suffix from "2MASS" designations.
    Returns
    -------
    Astropy table
    """

    n_sources = len(sources)

    Simbad.reset_votable_fields()
    Simbad.add_votable_fields('typed_id')  # keep search term in result table
    Simbad.add_votable_fields('ids')  # add all SIMBAD identifiers as an output column

    logger.info("simbad query started")
    result_table = Simbad.query_objects(sources['source'])
    logger.info("simbad query ended")

    ind = result_table['SCRIPT_NUMBER_ID'] > 0  # find indexes which contain results
    simbad_ids = result_table['TYPED_ID', 'IDS'][ind]

    db_names = []
    simbad_designations = []
    source_ids = []

    for row in simbad_ids:
        db_name = row['TYPED_ID']
        ids = row['IDS'].split('|')
        designation = [i for i in ids if desig_prefix in i]

        if designation:
            logger.debug(f'{db_name}, {designation[0]}')
            db_names.append(db_name)
            if len(designation) == 1:
                simbad_designations.append(designation[0])
            else:
                simbad_designations.append(designation[0])
                logger.warning(f'more than one designation matched, {designation}')

            if source_id_index is not None:
                source_id = designation[0].split()[source_id_index]
                source_ids.append(int(source_id))  # convert to int since long in Gaia

    n_matches = len(db_names)
    logger.info(f"Found, {n_matches}, {desig_prefix}, sources for, {n_sources}, sources")

    if source_id_index is not None:
        result_table = Table([db_names, simbad_designations, source_ids],
                             names=('db_names', 'designation', 'source_id'))
    else:
        result_table = Table([db_names, simbad_designations],
                             names=('db_names', 'designation'))

    return result_table


# SPECTRAL TYPES
def convert_spt_string_to_code(spectral_types):
    """
    # TODO: Could be part of future ingest_spectral_types function
    normal tests: M0, M5.5, L0, L3.5, T0, T3, T4.5, Y0, Y5, Y9.
    weird TESTS: sdM4, ≥Y4, T5pec, L2:, L0blue, Lpec, >L9, >M10, >L, T, Y
    digits are needed in current implementation.
    :param spectral_types:
    :return:
    """

    spectral_type_codes = []
    for spt in spectral_types:
        logger.debug(f"Trying to convert:, {spt}")
        spt_code = np.nan

        if spt == "":
            spectral_type_codes.append(spt_code)
            logger.debug("Appended NAN")
            continue

        # identify main spectral class, loop over any prefix text to identify MLTY
        for i, item in enumerate(spt):
            if item == 'M':
                spt_code = 60
                break
            elif item == 'L':
                spt_code = 70
                break
            elif item == 'T':
                spt_code = 80
                break
            elif item == 'Y':
                spt_code = 90
                break
        else:  # only trigger if not MLTY
            i = 0
        # find integer or decimal subclass and add to spt_code

        spt_code += float(re.findall('\d*\.?\d+', spt[i + 1:])[0])
        spectral_type_codes.append(spt_code)
        logger.debug(f"{spt}, {spt_code}")
    return spectral_type_codes


# PARALLAXES
def ingest_parallaxes(db, sources, plxs, plx_errs, plx_refs):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object
    sources: list[str]
        list of source names
    plxs: float or list[float]
        list of parallaxes corresponding to the sources
    plx_errs: float or list[float]
        list of parallaxes uncertainties
    plx_refs: str or list[str]
        list of references for the parallax data

    Examples
    ----------
    > ingest_parallaxes(db, my_sources, my_plx, my_plx_unc, my_plx_refs, verbose = True)

    """

    n_sources = len(sources)

    # Convert single element input value to list
    if isinstance(plx_refs, str):
        plx_refs = [plx_refs] * n_sources

    input_float_values = [plxs, plx_errs]
    for i, input_value in enumerate(input_float_values):
        if isinstance(input_value, float):
            input_value = [input_value] * n_sources
            input_float_values[i] = input_value
    plxs, plx_errs = input_float_values

    n_added = 0

    for i, source in enumerate(sources):  # loop through sources with parallax data to ingest
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise SimpleError(msg)
        else:
            db_name = db_name[0]

        # Search for existing parallax data and determine if this is the best
        # If no previous measurement exists, set the new one to the Adopted measurement
        adopted = None
        source_plx_data: Table = db.query(db.Parallaxes).filter(db.Parallaxes.c.source == db_name).table()

        if source_plx_data is None or len(source_plx_data) == 0:
            # if there's no other measurements in the database, set new data Adopted = True
            adopted = True
            # old_adopted = None  # not used
            logger.debug("No other measurement")
        elif len(source_plx_data) > 0:  # Parallax data already exists
            # check for duplicate measurement
            dupe_ind = source_plx_data['reference'] == plx_refs[i]
            if sum(dupe_ind):
                logger.debug(f"Duplicate measurement\n, {source_plx_data[dupe_ind]}")
                continue
            else:
                logger.debug("!!! Another Proper motion measurement exists,")
                if logger.level == 10:
                    source_plx_data.pprint_all()

            # check for previous adopted measurement and find new adopted
            adopted_ind = source_plx_data['adopted'] == 1
            if sum(adopted_ind):
                old_adopted = source_plx_data[adopted_ind]
                # if errors of new data are less than other measurements, set Adopted = True.
                if plx_errs[i] < min(source_plx_data['parallax_error']):
                    adopted = True

                    # unset old adopted
                    if old_adopted:
                        db.Parallaxes.update().where(and_(db.Parallaxes.c.source == old_adopted['source'][0],
                                                          db.Parallaxes.c.reference == old_adopted['reference'][0])). \
                            values(adopted=False).execute()
                        # check that adopted flag is successfully changed
                        old_adopted_data = db.query(db.Parallaxes).filter(
                            and_(db.Parallaxes.c.source == old_adopted['source'][0],
                                 db.Parallaxes.c.reference == old_adopted['reference'][0])).table()
                        logger.debug("Old adopted measurement unset")
                        if logger.level == 10:
                            old_adopted_data.pprint_all()

                logger.debug(f"The new measurement's adopted flag is:, {adopted}")
        else:
            msg = 'Unexpected state'
            logger.error(msg)
            raise RuntimeError(msg)

        # Construct data to be added
        parallax_data = [{'source': db_name,
                          'parallax': plxs[i],
                          'parallax_error': plx_errs[i],
                          'reference': plx_refs[i],
                          'adopted': adopted}]

        logger.debug(f"{parallax_data}")

        try:
            db.Parallaxes.insert().execute(parallax_data)
            n_added += 1
        except sqlalchemy.exc.IntegrityError:
            msg = "The source may not exist in Sources table.\n" \
                  "The parallax reference may not exist in Publications table. " \
                  "Add it with add_publication function. \n" \
                  "The parallax measurement may be a duplicate."
            logger.error(msg)
            raise SimpleError(msg)

    logger.info(f"Parallaxes added to database: {n_added}")

    return


# PROPER MOTIONS
def ingest_proper_motions(db, sources, pm_ras, pm_ra_errs, pm_decs, pm_dec_errs, pm_references):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object
    sources: list[str]
        list of source names
    pm_ras: list[float]
        list of proper motions in right ascension (RA)
    pm_ra_errs: list[float]
        list of uncertanties in proper motion RA
    pm_decs: list[float]
        list of proper motions in declination (dec)
    pm_dec_errs: list[float]
        list of uncertanties in proper motion dec
    pm_references: str or list[str]
        Reference or list of references for the proper motion measurements

    Examples
    ----------
    > ingest_proper_motions(db, my_sources, my_pm_ra, my_pm_ra_unc, my_pm_dec, my_pm_dec_unc, my_pm_refs,
                            verbose = True)

    """

    n_sources = len(sources)

    # Convert single element input value to list
    if isinstance(pm_references, str):
        pm_references = [pm_references] * len(sources)

    input_float_values = [pm_ras, pm_ra_errs, pm_decs, pm_dec_errs]
    for i, input_value in enumerate(input_float_values):
        if isinstance(input_value, float):
            input_value = [input_value] * n_sources
            input_float_values[i] = input_value
    pm_ras, pm_ra_errs, pm_decs, pm_dec_errs = input_float_values

    n_added = 0

    for i, source in enumerate(sources):

        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise SimpleError(msg)
        else:
            db_name = db_name[0]

        # Search for existing proper motion data and determine if this is the best
        # If no previous measurement exists, set the new one to the Adopted measurement
        # adopted = None  # not used
        source_pm_data = db.query(db.ProperMotions).filter(db.ProperMotions.c.source == db_name).table()
        if source_pm_data is None or len(source_pm_data) == 0:
            # if there's no other measurements in the database, set new data Adopted = True
            adopted = True
        elif len(source_pm_data) > 0:

            # check to see if other measurement is a duplicate of the new data
            dupe_ind = source_pm_data['reference'] == pm_references[i]
            if sum(dupe_ind):
                logger.debug(f"Duplicate measurement\n, {source_pm_data}")
                continue

            # check for previous adopted measurement
            adopted_ind = source_pm_data['adopted'] == 1
            if sum(adopted_ind):
                old_adopted = source_pm_data[adopted_ind]
            else:
                old_adopted = None

            # if errors of new data are less than other measurements, set Adopted = True.
            if pm_ra_errs[i] < min(source_pm_data['mu_ra_error']) and pm_dec_errs[i] < min(
                    source_pm_data['mu_dec_error']):
                adopted = True
                # unset old adopted if it exists
                if old_adopted:
                    db.ProperMotions. \
                        update().where(and_(db.ProperMotions.c.source == old_adopted['source'][0],
                                            db.ProperMotions.c.reference == old_adopted['reference'][0])). \
                        values(adopted=False).execute()
            else:
                adopted = False
                # if no previous adopted measurement, set adopted to the measurement with the smallest errors
                if not adopted and not old_adopted and \
                        min(source_pm_data['mu_ra_error']) < pm_ra_errs[i] and \
                        min(source_pm_data['mu_dec_error']) < pm_dec_errs[i]:
                    adopted_pm = db.ProperMotions.update().where(and_(db.ProperMotions.c.source == db_name,
                                                                      db.ProperMotions.c.mu_ra_error == min(
                                                                          source_pm_data['mu_ra_error']),
                                                                      db.ProperMotions.c.mu_dec_error == min(
                                                                          source_pm_data['mu_dec_error']))). \
                        values(adopted=True)
                    db.engine.execute(adopted_pm)
            logger.debug("!!! Another Proper motion exists")
            if logger.level == 10:
                source_pm_data.pprint_all()

        else:
            msg = 'Unexpected state'
            logger.error(msg)
            raise RuntimeError(msg)

        # Construct data to be added
        pm_data = [{'source': db_name,
                    'mu_ra': pm_ras[i],
                    'mu_ra_error': pm_ra_errs[i],
                    'mu_dec': pm_decs[i],
                    'mu_dec_error': pm_dec_errs[i],
                    'adopted': adopted,
                    'reference': pm_references[i]}]
        logger.debug(f'Proper motion data to add: {pm_data}')

        try:
            db.ProperMotions.insert().execute(pm_data)
            n_added += 1
        except sqlalchemy.exc.IntegrityError:
            msg = "The source may not exist in Sources table.\n" \
                  "The proper motion reference may not exist in Publications table. " \
                  "Add it with add_publication function. \n" \
                  "The proper motion measurement may be a duplicate."
            logger.error(msg)
            raise SimpleError(msg)

        updated_source_pm_data = db.query(db.ProperMotions).filter(db.ProperMotions.c.source == db_name).table()
        logger.info('Updated proper motion data:')
        if logger.level == 20:  # Info = 20, Debug = 10
            updated_source_pm_data.pprint_all()

    return


# PHOTOMETRY
def ingest_photometry(db, sources, bands, magnitudes, magnitude_errors, reference, ucds=None,
                      telescope=None, instrument=None, epoch=None, comments=None):
    """
    TODO: Write Docstring

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
    sources: list[str]
    bands: str or list[str]
    magnitudes: list[float]
    magnitude_errors: list[float]
    reference: str or list[str]
    ucds: str or list[str], optional
    telescope: str or list[str]
    instrument: str or list[str]
    epoch: list[float], optional
    comments: list[str], optional

    Returns
    -------

    """

    n_sources = len(sources)

    # Convert single element input values into lists
    input_values = [bands, reference, telescope, instrument, ucds]
    for i, input_value in enumerate(input_values):
        if isinstance(input_value, str):
            input_value = [input_value] * n_sources
            input_values[i] = input_value
    bands, reference, telescope, instrument, ucds = input_values

    input_float_values = [magnitudes, magnitude_errors]
    for i, input_value in enumerate(input_float_values):
        if isinstance(input_value, float):
            input_value = [input_value] * n_sources
            input_float_values[i] = input_value
    magnitudes, magnitude_errors = input_float_values

    if n_sources != len(magnitudes) or n_sources != len(magnitude_errors):
        msg = f"N Sources:, {len(sources)}," \
              f" N Magnitudes, {len(magnitudes)}, N Mag errors:, {len(magnitude_errors)}," \
              f"\nSources, magnitudes, and magnitude error lists should all be same length"
        logger.error(msg)
        raise RuntimeError(msg)

    if n_sources != len(reference) or n_sources != len(telescope) or n_sources != len(bands):
        msg = "All lists should be same length"
        logger.error(msg)
        raise RuntimeError(msg)

    n_added = 0

    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise SimpleError(msg)
        else:
            db_name = db_name[0]

        # if the uncertainty is masked, don't ingest anything
        if isinstance(magnitude_errors[i], np.ma.core.MaskedConstant):
            mag_error = None
        else:
            mag_error = str(magnitude_errors[i])

        # Construct data to be added
        photometry_data = [{'source': db_name,
                            'band': bands[i],
                            'ucd': ucds[i],
                            'magnitude': str(magnitudes[i]),  # Convert to string to maintain significant digits
                            'magnitude_error': mag_error,
                            'telescope': telescope[i],
                            'instrument': instrument[i],
                            'epoch': epoch,
                            'comments': comments,
                            'reference': reference[i]}]
        logger.debug(f'Photometry data: {photometry_data}')

        try:
            db.Photometry.insert().execute(photometry_data)
            n_added += 1
        except sqlalchemy.exc.IntegrityError:
            msg = "The source may not exist in Sources table.\n" \
                  "The reference may not exist in the Publications table. " \
                  "Add it with add_publication function. \n" \
                  "The measurement may be a duplicate."
            logger.error(msg)
            raise SimpleError(msg)

    logger.info(f"Photometry measurements added to database: {n_added}")

    return


# SPECTRA
def ingest_spectra(db, sources, spectra, regimes, telescopes, instruments, modes, obs_dates, references,
                   wavelength_units=None, flux_units=None, wavelength_order=None, local_spectra=None,
                   comments=None):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
    sources: list[str]
        List of source names
    spectra: list[str]
        List of filenames corresponding to spectra files
    regimes: str or list[str]
        List or string
    telescopes: str or list[str]
        List or string
    instruments: str or list[str]
        List or string
    modes: str or list[str]
        List or string
    obs_dates: str or datetime
        List of strings or datetime objects
    references: list[str]
        List or string
    wavelength_units: list[str] or Quantity, optional
        List or string
    flux_units: list[str] or Quantity, optional
        List or string
    wavelength_order: list[int], optional
    local_spectra: list[str], optional
    comments: list[str], optional
        List of strings

    """

    # Convert single value input values to lists
    input_values = [regimes, telescopes, instruments, modes, wavelength_order, wavelength_units, flux_units, references]
    for i, input_value in enumerate(input_values):
        if isinstance(input_value, str):
            input_value = [input_value] * len(sources)
            input_values[i] = input_value
    regimes, telescopes, instruments, modes, wavelength_order, wavelength_units, flux_units, references = input_values

    n_spectra = len(spectra)
    n_skipped = 0
    n_dupes = 0
    n_missing_instrument = 0
    n_added = 0
    n_blank = 0

    msg = f'Trying to add {n_spectra} spectra'
    logger.info(msg)

    for i, source in enumerate(sources):
        # TODO: check that spectrum can be read by astrodbkit

        # Get source name as it appears in the database
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise SimpleError(msg)
        else:
            db_name = db_name[0]

        # Find what spectra already exists in database for this source
        source_spec_data = db.query(db.Spectra).filter(db.Spectra.c.source == db_name).table()

        # SKIP if observation date is blank
        # TODO: try to populate obs date from meta data
        if ma.is_masked(obs_dates[i]) or obs_dates[i] == '':
            obs_date = None
            missing_obs_msg = f"Skipping spectrum with missing observation date: {source} \n"
            missing_row_spe = f"{source, obs_dates[i], references[i]} \n"
            logger.info(missing_obs_msg)
            logger.debug(missing_row_spe)
            n_blank += 1
            continue
        else:
            try:
                obs_date = pd.to_datetime(obs_dates[i])  # TODO: Another method that doesn't require pandas?
            except dateutil.parser._parser.ParserError:
                logger.warning(
                    f"Skipping {source} Cant convert obs date to Date Time object: {obs_dates[i]}")
                n_skipped += 1
                continue

        row_data = [{'source': db_name,
                     'spectrum': spectra[i],
                     'local_spectrum': None,  # if ma.is_masked(local_spectra[i]) else local_spectra[i],
                     'regime': regimes[i],
                     'telescope': telescopes[i],
                     'instrument': None if ma.is_masked(instruments[i]) else instruments[i],
                     'mode': None if ma.is_masked(modes[i]) else modes[i],
                     'observation_date': obs_date,
                     'wavelength_units': None,  # if ma.is_masked(wavelength_units[i]) else wavelength_units[i],
                     'flux_units': None,  # if ma.is_masked(flux_units[i]) else flux_units[i],
                     'wavelength_order': None,  # if ma.is_masked(wavelength_order[i]) else wavelength_order[i],
                     'comments': None if ma.is_masked(comments[i]) else comments[i],
                     'reference': references[i]}]
        logger.debug(row_data)

        try:
            db.Spectra.insert().execute(row_data)
            n_added += 1
        except sqlalchemy.exc.IntegrityError:
            # TODO: add elif to check if reference is in Publications Table

            # check telescope, instrument, mode exists
            telescope = db.query(db.Telescopes).filter(db.Telescopes.c.name == row_data[0]['telescope']).table()
            instrument = db.query(db.Instruments).filter(db.Instruments.c.name == row_data[0]['instrument']).table()
            mode = db.query(db.Modes).filter(db.Modes.c.name == row_data[0]['mode']).table()

            if len(source_spec_data) > 0:  # Spectra data already exists
                # check for duplicate measurement
                ref_dupe_ind = source_spec_data['reference'] == references[i]
                date_dupe_ind = source_spec_data['observation_date'] == obs_date
                instrument_dupe_ind = source_spec_data['instrument'] == instruments[i]
                mode_dupe_ind = source_spec_data['mode'] == modes[i]
                if sum(ref_dupe_ind) and sum(date_dupe_ind) and sum(instrument_dupe_ind) and sum(mode_dupe_ind):
                    msg = f"Skipping suspected duplicate measurement\n{source}\n"
                    msg2 = f"{source_spec_data[ref_dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference']}"
                    msg3 = f"{instruments[i], modes[i], obs_date, references[i], spectra[i]} \n"
                    logger.warning(msg)
                    logger.debug(msg2 + msg3)
                    n_dupes += 1
                    continue  # Skip duplicate measurement
                # else:
                #     msg = f'Spectrum could not be added to the database (other data exist): \n ' \
                #           f"{source, instruments[i], modes[i], obs_date, references[i], spectra[i]} \n"
                #     msg2 = f"Existing Data: \n "
                #            # f"{source_spec_data[ref_dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference', 'spectrum']}"
                #     msg3 = f"Data not able to add: \n {row_data} \n "
                #     logger.warning(msg + msg2)
                #     source_spec_data[ref_dupe_ind][
                #               'source', 'instrument', 'mode', 'observation_date', 'reference', 'spectrum'].pprint_all()
                #     logger.debug(msg3)
                #     n_skipped += 1
                #     continue
            if len(instrument) == 0 or len(mode) == 0 or len(telescope) == 0:
                msg = f'Spectrum for {source} could not be added to the database. \n' \
                      f' Telescope, Instrument, and/or Mode need to be added to the appropriate table. \n' \
                      f" Trying to find telescope: {row_data[0]['telescope']}, instrument: {row_data[0]['instrument']}, " \
                      f" mode: {row_data[0]['mode']} \n" \
                      f" Telescope: {telescope}, Instrument: {instrument}, Mode: {mode} \n"
                logger.error(msg)
                n_missing_instrument += 1
                continue
            else:
                msg = f'Spectrum for {source} could not be added to the database for unknown reason: \n {row_data} \n '
                logger.error(msg)
                raise SimpleError(msg)

    msg = f"SPECTRA ADDED: {n_added} \n" \
          f" Spectra with blank obs_date: {n_blank} \n" \
          f" Suspected duplicates skipped: {n_dupes}\n" \
          f" Missing Telescope/Instrument/Mode: {n_missing_instrument} \n" \
          f" Spectra skipped for unknown reason: {n_skipped} \n"
    logger.info(msg)

    if n_added + n_dupes + n_blank + n_skipped + n_missing_instrument != n_spectra:
        msg = "Numbers don't add up: "
        logger.error(msg)
        raise SimpleError(msg)

    spec_count = db.query(Spectra.regime, func.count(Spectra.regime)).group_by(Spectra.regime).all()
    # [(<Regime.mir: 'em.IR.MIR'>, 91), (<Regime.nir: 'em.IR.NIR'>, 450), (<Regime.optical: 'em.opt'>, 718)]

    spec_ref_count = db.query(Spectra.reference, func.count(Spectra.reference)). \
        group_by(Spectra.reference).order_by(func.count(Spectra.reference).desc()).limit(20).all()
    # [('Reid08b', 280), ('Cruz03', 191), ('Cruz18', 186), ('Cruz07', 158), ('Bard14', 57), ('Burg10a', 46),
    #  ('Cush06b', 30), ('Rayn09', 17), ('Kirk10', 15), ('Burg08d', 15), ('Burg04b', 15), ('PID51', 13), ('Kirk00', 11),
    #  ('PID3136', 10), ('Fili15', 10), ('CruzUnpub', 10), ('Burg06b', 10), ('Kirk08', 9), ('Cruz09', 8), ('Sieg07', 7)]

    telescope_spec_count = db.query(Spectra.telescope, func.count(Spectra.telescope)). \
        group_by(Spectra.telescope).order_by(func.count(Spectra.telescope).desc()).limit(20).all()
    # [('IRTF', 436), ('KPNO 4m', 251), ('CTIO 4m', 179), ('KPNO 2.1m', 93), ('Spitzer', 91), ('Keck I', 63),
    #  ('CTIO 1.5m', 48), ('Gemini South', 34), ('Gemini North', 27), ('ARC 3.5m', 14), ('Magellan II Clay', 11),
    #  ('ESO VLT U2', 7), ('Magellan I Baade', 5)]

    logger.info(f'Spectra in the database: \n {spec_count} \n {spec_ref_count} \n {telescope_spec_count}')

    return


def ingest_instrument(db, telescope=None, instrument=None, mode=None):
    """
    Script to ingest instrumentation

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    telescope: str
    instrument: str
    mode: str

    Returns
    -------

    None

    """

    # Make sure a search term is provided
    if telescope is None and instrument is None and mode is None:
        msg = "Telescope, Instrument, and Mode must be provided"
        logger.error(msg)
        raise SimpleError(msg)

    msg_search = f'Searching for {telescope},{instrument},{mode} in database'
    logger.info(msg_search)

    # Search that the inputs are in the database
    telescope_db = db.query(db.Telescopes).filter(db.Telescopes.c.name == telescope).table()
    instrument_db = db.query(db.Instruments).filter(db.Instruments.c.name == instrument).table()
    mode_db = db.query(db.Modes).filter(db.Modes.c.name == mode).table()

    # If they are in database then logger message of " these have already been ingested"

    # Else
    # Make a dictionary with the inputs
    if len(telescope_db) == 1 and len(instrument_db) == 1 and len(mode_db) == 1:
        msg_found = f'{telescope},{instrument}, and {mode} have already been ingested.'
        logger.info(msg_found)
        return

    if len(telescope_db) == 0:
        telescope_add = [{'name': telescope}]
        try:
            db.Telescopes.insert().execute(telescope_add)
            msg_telescope = f'{telescope} was successfully ingested in the database'
            logger.info(msg_telescope)
        except sqlalchemy.exc.IntegrityError as e:
            if telescope is None:
                msg = 'Telescope name must be provided to ingest a telescope.'
                logger.error(msg)
                raise SimpleError(msg + '\n' + str(e))
            else:
                msg = 'Telescope could not be ingested for unknown reason.'
                logger.error(msg)
                raise SimpleError(msg + '\n' + str(e))

    if len(instrument_db) == 0:
        instrument_add = [{'name': instrument}]
        try:
            db.Instruments.insert().execute(instrument_add)
            msg_instrument = f'{instrument} was successfully ingested in the database.'
            logger.info(msg_instrument)
        except sqlalchemy.exc.IntegrityError as e:
            if instrument is None:
                msg = 'Instrument name must be provided to ingest an instrument.'
                logger.error(msg)
                raise SimpleError(msg + '\n' + str(e))
            else:
                msg = 'Instrument could not be ingested for unknown reason.'
                logger.error(msg)
                raise SimpleError(msg + '\n' + str(e))

    if len(mode_db) == 0:
        mode_add = [{'name': mode,
                     'instrument': instrument,
                     'telescope': telescope}]
        try:
            db.Modes.insert().execute(mode_add)
            msg_mode = f'{mode} was successfully ingested in the database'
            logger.info(msg_mode)
        except sqlalchemy.exc.IntegrityError as e:
            if telescope and instrument is None or instrument is None or telescope is None:
                msg = 'Telescope and Instrument must be provided to ingest a mode'
                logger.error(msg)
                raise SimpleError(msg + '\n' + str(e))
            else:
                msg = 'Mode could not be ingested for unknown reason.'
                logger.error(msg)
                raise SimpleError(msg + '\n' + str(e))
    return
