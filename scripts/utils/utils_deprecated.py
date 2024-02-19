from astropy.table import Table

from scripts.ingests.ingest_utils import logger
from scripts.ingests.utils import find_source_in_db, SimpleError


def sort_sources(db, ingest_names: list,
                 ingest_ras: list = None, ingest_decs: list = None, search_radius: float = 60.):
    """
    Classifying sources to be ingested into the database into three categories:
    1) in the database with the same name,
    2) in the database with a different name, or
    3) not in the database and need to be added.

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    ingest_names: list
        Names of sources
    ingest_ras: list (optional)
        Right ascensions of sources. Decimal degrees.
    ingest_decs: list (optional)
        Declinations of sources. Decimal degrees.
    search_radius: float
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


# Make sure all source names are Simbad resolvable:
def check_names_simbad(ingest_names, ingest_ra, ingest_dec, radius='2s'):
    resolved_names = []
    n_sources = len(ingest_names)
    n_name_matches = 0
    n_selections = 0
    n_nearby = 0
    n_notfound = 0

    for i, ingest_name in enumerate(ingest_names):
        # Query Simbad for identifiers matching the ingest source name
        identifer_result_table = Simbad.query_object(ingest_name, verbose=False)

        # Successfully resolved one matching identifier in Simbad
        if identifer_result_table is not None and len(identifer_result_table) == 1:
            # Add the Simbad resolved identifier ot the resolved_name list and deals with unicode
            if isinstance(identifer_result_table['MAIN_ID'][0], str):
                resolved_names.append(identifer_result_table['MAIN_ID'][0])
            else:
                resolved_names.append(identifer_result_table['MAIN_ID'][0].decode())
            logger.debug(f"{resolved_names[i]}, Found name match in Simbad")
            n_name_matches = n_name_matches + 1

        # If no identifier match found, search within "radius" of coords for a Simbad object
        else:
            logger.debug(f"searching around, {ingest_name}")
            coord_result_table = Simbad.query_region(
                SkyCoord(ingest_ra[i], ingest_dec[i], unit=(u.deg, u.deg), frame='icrs'),
                radius=radius)

            # If no match is found in Simbad, use the name in the ingest table
            if coord_result_table is None:
                resolved_names.append(ingest_name)
                logger.debug("coord search failed")
                n_notfound = n_notfound + 1

            # If more than one match found within "radius", query user for selection and append to resolved_name
            elif len(coord_result_table) > 1:
                for j, name in enumerate(coord_result_table['MAIN_ID']):
                    logger.debug(f'{j}: {name}')
                selection = int(input('Choose \n'))
                if isinstance(coord_result_table['MAIN_ID'][selection], str):
                    resolved_names.append(coord_result_table['MAIN_ID'][selection])
                else:
                    resolved_names.append(coord_result_table['MAIN_ID'][selection].decode())
                logger.debug(f"{resolved_names[i]}, you selected")
                n_selections = n_selections + 1

            # If there is only one match found, accept it and append to the resolved_name list
            elif len(coord_result_table) == 1:
                if isinstance(coord_result_table['MAIN_ID'][0], str):
                    resolved_names.append(coord_result_table['MAIN_ID'][0])
                else:
                    resolved_names.append(coord_result_table['MAIN_ID'][0].decode())
                logger.debug(f"{resolved_names[i]}, only result nearby in Simbad")
                n_nearby = n_nearby + 1

    # Report how many find via which methods
    logger.info(f"Names Found:, {n_name_matches}")
    logger.info(f"Names Selected, {n_selections}")
    logger.info(f"Names Found, {n_nearby}")
    logger.info(f"Not found, {n_notfound}")

    n_found = n_notfound + n_name_matches + n_selections + n_nearby
    logger.warning('problem' if n_found != n_sources else (str(n_sources) + 'names'))

    return resolved_names


def test_add_names(db):
    sources_2 = ['Fake 1', 'Fake 2']
    other_names_2 = ['Fake 1 alt', 'Fake 2 alt']
    other_names_3 = ['Fake 1 alt', 'Fake 2 alt', 'Fake 3 alt']
    alt_names_table = Table(names=('db_name', 'ingest_name'), dtype=('str', 'str'))
    alt_names_table.add_row(('Fake 1', 'Fake 1 alternate'))
    alt_names_table.add_row(('Fake 2', 'Fake 2 alternate'))

    alt_names_table3 = [('Fake 1', 'Fake 1 alternate', '3rd column')]

    # Add names using two lists
    add_names(db, sources=sources_2, other_names=other_names_2)
    results = db.query(db.Names).filter(db.Names.c.source == 'Fake 1').table()
    assert results['other_name'][0] == 'Fake 1 alt'
    results = db.query(db.Names).filter(db.Names.c.source == 'Fake 2').table()
    assert results['other_name'][0] == 'Fake 2 alt'

    # Add names using a table
    add_names(db, names_table=alt_names_table)
    results = db.query(db.Names).filter(db.Names.c.source == 'Fake 1').table()
    assert results['other_name'][1] == 'Fake 1 alternate'
    results = db.query(db.Names).filter(db.Names.c.source == 'Fake 2').table()
    assert results['other_name'][1] == 'Fake 2 alternate'

    # should fail if lists are different length
    with pytest.raises(RuntimeError):
        add_names(db, sources=sources_2, other_names=other_names_3)

    # should fail if both table and sources list are given
    with pytest.raises(RuntimeError):
        add_names(db, sources=sources_2, other_names=other_names_2, names_table=alt_names_table)

        # should fail if table has three columns
        with pytest.raises(RuntimeError):
            add_names(db, names_table=alt_names_table3)
