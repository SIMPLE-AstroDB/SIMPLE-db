import numpy as np
import re
from astropy.coordinates import SkyCoord
import astropy.units as u
from astroquery.simbad import Simbad
from astropy.table import Table, vstack
import warnings
warnings.filterwarnings("ignore", module='astroquery.simbad')
from sqlalchemy import or_
import ads
import os


def search_publication(db, name=None, doi=None, bibcode=None):
    """
    Find publications in the database by matching on the publication name,  doi, or bibcode

    Parameters
    ----------
    db
        Variable referencing the database to search
    name: str
        Name of publication to search
    doi: str
        DOI of publication to search
    bibcode: str
        ADS Bibcode of publication to search

    Returns
    -------
    Table containing publications matching name

    Example
    -------
    >>> test = search_publication(db, name='Martin19')
    Searching Martin19
    No matching publications for Martin19
    Trying Mart
    Found 7 matching publications for Mart
      name         bibcode                     doi                                                           description
    ------- --------------------- ----------------------------- ------------------------------------------------------------------------------------------------------
    Mart99a   1999Sci...283.1718M 10.1126/science.283.5408.1718                        A Search for Companions to Nearby Brown Dwarfs: The Binary DENIS-P J1228.2-1547
    Mart99b   1999AJ....118.2466M                10.1086/301107                                              Spectroscopic Classification of Late-M and L Field Dwarfs
     Mart00   2000ApJ...529L..37M                10.1086/312450 The Discovery of a Companion to the Very Cool Dwarf Gliese 569B with the Keck Adaptive Optics Facility
     Mart06   2006A&A...456..253M    10.1051/0004-6361:20054186                                         Resolved Hubble space spectroscopy of ultracool binary systems
     Mart98 1998ApJ...507L..41M                  10.1086/311675                                                           The First L-Type Brown Dwarf in the Pleiades
     Mart04   2004SPIE.5492.1653M             10.1117/12.551828                                              PANIC: a near-infrared camera for the Magellan telescopes
     Mart18                  None                          None                                                                                                   None
    """

    # Make sure a search term is provided
    if name is None and doi is None and bibcode is None:
        print("Name, Bibcode, or DOI must be provided")
        return

    not_null_pub_filters = []
    if name:
        fuzzy_query_name = '%' + name + '%'
        not_null_pub_filters.append(db.Publications.c.name.ilike(fuzzy_query_name))
    if doi:
        not_null_pub_filters.append(db.Publications.c.doi.ilike(doi))
    if bibcode:
        not_null_pub_filters.append(db.Publications.c.bibcode.ilike(bibcode))

    if len(not_null_pub_filters) > 0:
        pub_search_table = db.query(db.Publications).filter(or_(*not_null_pub_filters)).table()

    n_pubs_found = len(pub_search_table)

    if n_pubs_found > 0:
        print(f'Found {n_pubs_found} matching publications for {name} or {doi} or {bibcode}')

    # If no matches found, search using first four characters of input name
    if n_pubs_found == 0 and name:
        shorter_name = name[:4]
        print(f'No matching publications for {name}, Trying {shorter_name}')
        fuzzy_query_shorter_name = '%' + shorter_name + '%'
        pub_search_table = db.query(db.Publications).filter(db.Publications.c.name.ilike(fuzzy_query_shorter_name)).table()
        n_pubs_found_short = len(pub_search_table)
        if n_pubs_found_short == 0:
            print(f'No matching publications for {shorter_name}')
            print('Use add_publication() to add it to the database.')
        if n_pubs_found_short > 0:
            print(f'Found {n_pubs_found_short} matching publications for {shorter_name}')
            #pub_search_table.pprint_all()

    if len(pub_search_table) > 0:
        pub_search_table.pprint_all()

    return pub_search_table


def add_publication(db, doi=None, bibcode=None, name=None, description=None, dryrun=True):
    """

    Parameters
    ----------
    db
    doi: str
    bibcode: str
    name: str
    description: str
    dryrun: bool
        Default is to list change but not modify the database. Set dryrun=False to actually change the database.

    Returns
    -------
    None

    """

    if not(doi or bibcode):
        print('DOI or Bibcode is required input')
        return

    ads.config.token = os.getenv('ADS_TOKEN')

    # check to make sure it doesn't already exist
    not_null_pub_filters = []
    if name:
        not_null_pub_filters.append(db.Publications.c.name == name)
    if doi:
        not_null_pub_filters.append(db.Publications.c.doi == doi)
    if bibcode:
        not_null_pub_filters.append(db.Publications.c.bibcode == bibcode)

    pub_search_table = db.query(db.Publications).filter(or_(*not_null_pub_filters)).table()

    if len(pub_search_table) > 0:
        print('Similar publication already exists in database')
        pub_search_table.pprint_all()
        return

    if doi:
        doi_matches = ads.SearchQuery(doi=doi)
        doi_matches_list = list(doi_matches)
        if len(doi_matches_list) != 1:
            print('should only be one matching doi')
            return

        if len(doi_matches_list) == 1:
            print("Publication found in ADS")
            article = doi_matches_list[0]
            print(article.first_author, article.year, article.bibcode, article.title)
            name = article.first_author[0:4] + article.year[-2]
            description = article.title[0]

    if bibcode:
        bibcode_matches = ads.SearchQuery(bibcode=bibcode)

    #check again to make sure publication does not already exist in database
    not_null_pub_filters = []
    if name:
        not_null_pub_filters.append(db.Publications.c.name == name)
    if doi:
        not_null_pub_filters.append(db.Publications.c.doi == doi)
    if bibcode:
        not_null_pub_filters.append(db.Publications.c.bibcode == bibcode)

    pub_search_table = db.query(db.Publications).filter(or_(*not_null_pub_filters)).table()

    if len(pub_search_table) > 0:
        print('Similar publication already exists in database')
        pub_search_table.pprint_all()
        return

    if dryrun:
        print("name:", name, "\nbibcode:", article.bibcode, "\ndoi:", doi, "\ndescription:", description)
        print("re-run with dryrun=False to add to the database")

    if dryrun is False:
        new_ref = [{'name': name, 'bibcode': article.bibcode, 'doi': doi, 'description': description}]
        db.Publications.insert().execute(new_ref)
        print(f'Added {name} to Publications table')

    #     add_doi_bibcode = db.Publications.update().where(db.Publications.c.name == 'Manj19'). \
    #         values(bibcode='2019AJ....157..101M', doi='10.3847/1538-3881/aaf88f',
    #               description='Cloud Atlas: HST nir spectral library')
    #     db.engine.execute(add_doi_bibcode)


# Make sure all source names are Simbad resolvable:
def check_names_simbad(ingest_names, ingest_ra, ingest_dec, radius='2s', verbose=False):
    verboseprint = print if verbose else lambda *a, **k: None

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
            verboseprint(resolved_names[i], "Found name match in Simbad")
            n_name_matches = n_name_matches + 1

        # If no identifier match found, search within "radius" of coords for a Simbad object
        else:
            verboseprint("searching around ", ingest_name)
            coord_result_table = Simbad.query_region(
                SkyCoord(ingest_ra[i], ingest_dec[i], unit=(u.deg, u.deg), frame='icrs'),
                radius=radius, verbose=verbose)
                
            # If no match is found in Simbad, use the name in the ingest table
            if coord_result_table is None:
                resolved_names.append(ingest_name)
                verboseprint("coord search failed")
                n_notfound = n_notfound + 1
                
            # If more than one match found within "radius", query user for selection and append to resolved_name
            elif len(coord_result_table) > 1:
                for j, name in enumerate(coord_result_table['MAIN_ID']):
                    print(f'{j}: {name}')
                selection = int(input('Choose \n'))
                if isinstance(coord_result_table['MAIN_ID'][selection], str):
                    resolved_names.append(coord_result_table['MAIN_ID'][selection])
                else:
                    resolved_names.append(coord_result_table['MAIN_ID'][selection].decode())
                verboseprint(resolved_names[i], "you selected")
                n_selections = n_selections + 1

            # If there is only one match found, accept it and append to the resolved_name list
            elif len(coord_result_table) == 1:
                if isinstance(coord_result_table['MAIN_ID'][0], str):
                    resolved_names.append(coord_result_table['MAIN_ID'][0])
                else:
                    resolved_names.append(coord_result_table['MAIN_ID'][0].decode())
                verboseprint(resolved_names[i], "only result nearby in Simbad")
                n_nearby = n_nearby + 1

    # Report how many find via which methods
    print("Names Found:", n_name_matches)
    print("Names Selected", n_selections)
    print("Names Found", n_nearby)
    print("Not found", n_notfound)

    n_found = n_notfound + n_name_matches + n_selections + n_nearby
    print('problem' if n_found != n_sources else (n_sources, 'names'))

    return resolved_names


def convert_spt_string_to_code(spectral_types, verbose=False):
    """
    normal tests: M0, M5.5, L0, L3.5, T0, T3, T4.5, Y0, Y5, Y9.
    weird TESTS: sdM4, ≥Y4, T5pec, L2:, L0blue, Lpec, >L9, >M10, >L, T, Y
    digits are needed in current implementation.
    :param spectral_types:
    :param verbose:
    :return:
    """

    verboseprint = print if verbose else lambda *a, **k: None

    spectral_type_codes = []
    for spt in spectral_types:
        verboseprint("Trying to convert:", spt)
        spt_code = np.nan

        if spt == "":
            spectral_type_codes.append(spt_code)
            verboseprint("Appended NAN")
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
        # find integer or decimal subclass and add to spt_code

        spt_code += float(re.findall('\d*\.?\d+', spt[i + 1:])[0])
        spectral_type_codes.append(spt_code)
        verboseprint(spt, spt_code)
    return spectral_type_codes


def ingest_parallaxes(db, sources, plx, plx_unc, plx_ref, verbose=False, norun=False):
    """

    TODO: do stuff about adopted in cases of multiple measurements.

    :param db:
    :param sources:
    :param plx:
    :param plx_unc:
    :param plx_ref:
    :param verbose:
    :param norun:
    :return:
    """
    verboseprint = print if verbose else lambda *a, **k: None

    n_added = 0

    for i, source in enumerate(sources):
        db_name = db.search_object(source, output_table='Sources')[0]['source']

        # Search for existing parallax data and determine if this is the best
        adopted = None
        source_plx_data = db.query(db.Parallaxes).filter(db.Parallaxes.c.source == db_name).table()
        if source_plx_data is None or len(source_plx_data) == 0:
            adopted = True
        else:
            print("OTHER PARALLAX EXISTS")
            print(source_plx_data)

        # TODO: Work out logic for updating/setting adopted. Be it's own function.

        # TODO: Make function which validates refs

        # Construct data to be added
        parallax_data = [{'source': db_name,
                          'parallax': plx[i],
                          'parallax_error': plx_unc[i],
                          'reference': plx_ref[i],
                          'adopted': adopted}]
        verboseprint(parallax_data)

        # Consider making this optional or a key to only view the output but not do the operation.
        if not norun:
            db.Parallaxes.insert().execute(parallax_data)
            n_added += 1

    print("Added to database: ", n_added)
