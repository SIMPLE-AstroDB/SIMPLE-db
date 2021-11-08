"""
Utils functions for use in ingests
"""
from scripts.ingests.utils import *
import logging
import numpy as np
import numpy.ma as ma
from astropy.table import Table, unique
import re

logger = logging.getLogger('SIMPLE')


# SOURCES
def ingest_sources(db, sources, ras, decs, references, comments=None, epochs=None,
                   equinoxes=None, raise_error=True):
    """
    Script to ingest sources

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    sources: list of strings
        Names of sources
    ras: list of floats
        Right ascensions of sources. Decimal degrees.
    decs: list of floats
        Declinations of sources. Decimal degrees.
    references: list of strings
        Discovery references of sources
    comments: list of strings
        Comments
    epochs: list of floats
        Epochs of coordinates
    equinoxes: list of floats
        Equinoxes of coordinates
    raise_error: bool
        True (default): Raise an error if a source cannot be ingested
        False: Log a warning but skip sources which cannot be ingested

    Returns
    -------

    None

    """
    # TODO: add example

    n_added = 0
    n_existing = 0
    n_names = 0
    n_alt_names = 0
    n_skipped = 0
    n_sources = len(sources)
    n_multiples = 0

    logger.info(f"Trying to add {n_sources} sources")

    if epochs is None:
        epochs = [None] * n_sources
    if equinoxes is None:
        equinoxes = [None] * n_sources
    if comments is None:
        comments = [None] * n_sources

    # Loop over each source and decide to ingest, skip, or add alt name
    for i, source in enumerate(sources):
        # Find out if source is already in database or not
        name_matches = find_source_in_db(db, source, ra=ras[i], dec=decs[i])

        if len(name_matches) == 1: # Source is already in database
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
                except sqlalchemy.exc.IntegrityError:
                    msg = f"{i}: Could not add {alt_names_data} to database"
                    logger.warning(msg)
                    if raise_error:
                        raise SimpleError(msg)
                    else:
                        continue
            continue # Source is already in database, nothing new to ingest
        elif len(name_matches) > 1: # Multiple source matches in the database
            n_multiples += 1
            msg1 = f"{i} Skipping {source} "
            msg = f"{i} More than one match for {source}\n {name_matches}\n"
            logger.warning(msg1+msg)
            if raise_error:
                raise SimpleError(msg)
            else:
                continue
        elif len(name_matches) == 0: # No match in the database, INGEST!
            logger.debug(f"{i}: Not in database: {source}")

            # Construct data to be added
            source_data = [{'source': source,
                            'ra': ras[i],
                            'dec': decs[i],
                            'reference': references[i],
                            'epoch': None if ma.is_masked(epochs[i]) else epochs[i],
                            'equinox': None if ma.is_masked(equinoxes[i]) else equinoxes[i],
                            'comments': None if ma.is_masked(comments[i]) else comments[i]}]
            names_data = [{'source': source,
                           'other_name': source}]
        else:
            msg = f"{i}: unexpected condition encountered ingesting {source}"
            logger.error(msg)
            raise SimpleError(msg)

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
                    raise SimpleError(msg+msg2)
                else:
                    continue

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
    db
        Database object
    sources
        list of source names
    plxs
        list of parallaxes corresponding to the sources
    plx_errs
        list of parallaxes uncertainties
    plx_refs
        list of references for the parallax data

    Examples
    ----------
    > ingest_parallaxes(db, my_sources, my_plx, my_plx_unc, my_plx_refs, verbose = True)

    """

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
    db
        Database object
    sources
        list of source names
    pm_ras
        list of proper motions in right ascension (RA)
    pm_ra_errs
        list of uncertanties in proper motion RA
    pm_decs
        list of proper motions in declination (dec)
    pm_dec_errs
        list of uncertanties in proper motion dec
    pm_references
        list of references for the proper motion measurements

    Examples
    ----------
    > ingest_proper_motions(db, my_sources, my_pm_ra, my_pm_ra_unc, my_pm_dec, my_pm_dec_unc, my_pm_refs,
                            verbose = True)

    """

    n_added = 0

    for i, source in enumerate(sources):

        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise SimpleError(msg)
        else:
            db_name= db_name[0]

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
        logger.debug('Updated proper motion data:')
        if logger.level == 10:
            updated_source_pm_data.pprint_all()

    return


# PHOTOMETRY
def ingest_photometry(db, sources, bands, magnitudes, magnitude_errors, reference, ucds=None,
                      telescope=None, instrument=None, epoch=None, comments=None):
    """
    TODO: Write Docstring

    Parameters
    ----------
    db
    sources
    bands
    magnitudes
    magnitude_errors
    reference
    ucds
    telescope
    instrument
    epoch
    comments

    Returns
    -------

    """

    n_added = 0

    n_sources = len(sources)

    if n_sources != len(magnitudes) or n_sources != len(magnitude_errors):
        msg = f"N Sources:, {len(sources)}," \
              f" N Magnitudes, {len(magnitudes)}, N Mag errors:, {len(magnitude_errors)}," \
              f"\nSources, magnitudes, and magnitude error lists should all be same length"
        logger.error(msg)
        raise RuntimeError(msg)

    if isinstance(bands, str):
        bands = [bands] * len(sources)

    if isinstance(reference, str):
        reference = [reference] * len(sources)

    if isinstance(telescope, str):
        telescope = [telescope] * len(sources)

    if isinstance(instrument, str):
        instrument = [instrument] * len(sources)

    if isinstance(ucds, str):
        ucds = [ucds] * len(sources)

    if n_sources != len(reference) or n_sources != len(telescope) or n_sources != len(bands):
        msg = "All lists should be same length"
        logger.error(msg)
        raise RuntimeError(msg)

    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise SimpleError(msg)
        else:
            db_name= db_name[0]

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
