"""
Utils functions for use in ingests
"""
from scripts.ingests.utils import *
import logging
import numpy as np
import numpy.ma as ma
from tqdm import tqdm
from astropy.table import Table, unique

logger = logging.getLogger('SIMPLE')


# SOURCES
def sort_sources(db, ingest_names, ingest_ras=None, ingest_decs=None, search_radius=60.):
    """
    Classifying sources to be ingested into the database into three categories:
    1) in the database with the same name,
    2) in the database with a different name, or
    3) not in the database and need to be added.

    Parameters
    ----------
    db
    ingest_names
        Names of sources
    ingest_ras: (optional)
        Right ascensions of sources. Decimal degrees.
    ingest_decs: (optional)
        Declinations of sources. Decimal degrees.
    search_radius
        radius in arcseconds to use for source matching

    Returns
    -------
    missing_sources_index
        Indices of sources which are not in the database
    existing_sources_index
        Indices of sources which are already in the database
    alt_names_table
        Astropy Table with Other Names to add to database
        Can be used as input to add_names function
    """

    n_sources = len(ingest_names)
    logger.info(f"SORTING {n_sources} SOURCES\n")

    if ingest_ras is None and ingest_decs is None:
        coords = False
    else:
        coords = True

    existing_sources_index = []
    n_multiples = 0
    missing_sources_index = []
    alt_names_table = Table(names=('db_name', 'ingest_name'), dtype=('str', 'str'))

    for i, name in enumerate(ingest_names):

        logger.debug(f"{i}: sorting {name}")

        if coords:
            name_matches = find_source_in_db(db, name, ingest_ra=ingest_ras[i], ingest_dec=ingest_decs[i],
                                             search_radius=search_radius)
        else:
            name_matches = find_source_in_db(db, name)

        if len(name_matches) == 1:
            existing_sources_index.append(i)
            logger.debug(f"{i}, match found for {name}: {name_matches[0]}")
            # Figure out if ingest name is an alternate name
            db_matches = db.search_object(name, output_table='Sources', fuzzy_search=False)
            if len(db_matches) == 0:
                alt_names_table.add_row([name_matches, name])
        elif len(name_matches) > 1:
            existing_sources_index.append(i)
            n_multiples += 1
            msg = f"{i}, More than one match for {name}\n {name_matches}\n"
            logger.warning(msg)
        elif len(name_matches) == 0:
            logger.debug(f"{i}: Not in database: {name}")
            missing_sources_index.append(i)
        else:
            msg = f"{i}: unexpected condition encountered sorting {name}"
            logger.error(msg)
            raise SimpleError(msg)

    n_ingest = len(ingest_names)
    n_existing = len(existing_sources_index)
    n_alt = len(alt_names_table)
    n_missing = len(missing_sources_index)

    if n_ingest != n_existing + n_missing:
        msg = "Unexpected number of sources"
        logger.error(msg)
        raise RuntimeError("Unexpected number of sources")

    print(' ')
    logger.info(f"ALL SOURCES SORTED: {n_sources}")
    logger.info(f"Sources already in database: {n_existing}")
    logger.info(f"Sources with multiple matches in database: {n_multiples}")
    logger.info(f"Sources found with alternate names: {n_alt}")
    logger.info(f"Sources not found in the database: {n_missing}\n")

    logger.debug(f"Existing Sources:\n, {ingest_names[existing_sources_index]}")
    logger.debug(f"Missing Sources:\n, {ingest_names[missing_sources_index]}")
    logger.debug("Existing Sources with different name:\n")
    if logger.level == 10:  # debug
        alt_names_table.pprint_all()

    return missing_sources_index, existing_sources_index, alt_names_table


def ingest_sources(db, sources, ras, decs, references, comments=None, epochs=None,
                   equinoxes=None):
    """
    Script to ingest sources

    TODO: Make a keyword which toggles warnings/errors for sources and names which aren't ingested

    Parameters
    ----------
    db
    sources
    ras
    decs
    references
    comments
    epochs
    equinoxes

    Returns
    -------

    """
    # TODO: add example

    n_added = 0
    n_names = 0
    n_skipped = 0
    n_sources = len(sources)

    logger.info(f"Trying to add {n_sources} sources")

    if epochs is None:
        epochs = [None] * n_sources
    if equinoxes is None:
        equinoxes = [None] * n_sources
    if comments is None:
        comments = [None] * n_sources

    for i, source in enumerate(sources):

        # Construct data to be added
        source_data = [{'source': sources[i],
                        'ra': ras[i],
                        'dec': decs[i],
                        'reference': references[i],
                        'epoch': epochs[i],
                        'equinox': equinoxes[i],
                        'comments': None if ma.is_masked(comments[i]) else comments[i]}]
        # logger.debug(str(source_data))

        names_data = [{'source': sources[i],
                       'other_name': sources[i]}]

        try:
            db.Sources.insert().execute(source_data)
            n_added += 1
            msg = f"Added {str(source_data)}"
            logger.debug(msg)
        except sqlalchemy.exc.IntegrityError:
            # try reference without last letter e.g.Smit04 instead of Smit04a
            if ma.is_masked(source_data[0]['reference']):
                msg = f"Skipping: {sources[i]}. Discovery reference is blank. \n"
                msg2 = f"\n {str(source_data)}\n"
                logger.warning(msg)
                logger.debug(msg2)
                n_skipped += 1
                continue
            elif source_data[0]['reference'][-1] in ('a', 'b'):
                source_data[0]['reference'] = references[i][:-1]
                try:
                    db.Sources.insert().execute(source_data)
                    n_added += 1
                    msg = f"Added \n {str(source_data)}"
                    logger.debug(msg)
                except sqlalchemy.exc.IntegrityError:
                    msg = f"Skipping {sources[i]} "
                    msg2 = f"\n {str(source_data)} " \
                           f"\n Discovery reference may not exist in the Publications table. " \
                           "(Add it with add_publication function.) \n "
                    logger.warning(msg)
                    logger.debug(msg2)
                    n_skipped += 1
                    continue
            else:
                msg = f"Skipping: {sources[i]}"
                msg2 = f"\n {str(source_data)} " \
                       f"\n Possible duplicate source or discovery reference may not exist in the Publications table." \
                       f"\n Add it with add_publication function. \n "
                logger.warning(msg)
                logger.debug(msg2)
                n_skipped += 1
                continue
                # raise SimpleError(msg)

        try:
            db.Names.insert().execute(names_data)
            logger.debug(f"Name added to database: {names_data}\n")
            n_names += 1
        except sqlalchemy.exc.IntegrityError:
            msg = f"Could not add {names_data} to database"
            logger.warning(msg)


    logger.info(f"Sources added to database: {n_added}")
    logger.info(f"Names added to database: {n_names} \n")
    logger.info(f"Sources NOT added to database: {n_skipped} \n")

    return


def add_names(db, sources=None, other_names=None, names_table=None):
    """
    Add source names to the Names table in the database.
    Provide either two lists of sources and other_names or a 2D names_table.

    Parameters
    ----------
    db
    sources
        list of source names which already exist in the database
    other_names
        list of alternate names for sources
    names_table
        table with source and other_names.
        Expecting source name to be first column and other_names in the 2nd.
    """

    if names_table is not None and sources is not None:
        msg = "Both names table and sources list provided. Provide one or the other"
        logger.error(msg)
        raise RuntimeError(msg)

    names_data = []

    if sources is not None or other_names is not None:
        # Length of sources and other_names list should be equal
        if len(sources) != len(other_names):
            msg = "Length of sources and other_names should be equal"
            logger.error(msg)
            raise RuntimeError(msg)

        for source, other_name in zip(sources, other_names):
            names_data.append({'source': source, 'other_name': other_name})

    if names_table is not None:
        if len(names_table) == 0:
            msg = "No new names to add to database"
            logger.warning(msg)
        elif len(names_table[0]) != 2:
            msg = "Each tuple should have two elements"
            logger.error(msg)
            raise RuntimeError(msg)
        else:
            # Remove duplicate names
            names_table = unique(names_table)

        for name_row in names_table:
            names_data.append({'source': name_row[0], 'other_name': name_row[1]})
            logger.debug(name_row)

    n_names = len(names_data)

    if n_names > 0:
        try:
            db.Names.insert().execute(names_data)
            logger.info(f"Names added to database: {n_names}\n")
        except sqlalchemy.exc.IntegrityError:
            msg = f"Could not add {n_names} alt names to database"
            logger.warning(msg)

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
        # TODO: replace with find_source_in_db
        db_name = db.search_object(source, output_table='Sources')[0]['source']

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
        # TODO: replace with find_source_in_db
        db_name = db.search_object(source, output_table='Sources')[0]['source']

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
