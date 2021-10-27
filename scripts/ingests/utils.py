"""
Utils functions for use in ingests
"""
import logging
import os
import sys
import re
import warnings
from pathlib import Path
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
import ads
from astropy.coordinates import SkyCoord
import astropy.units as u
from astroquery.simbad import Simbad
from astropy.table import Table, unique
from sqlalchemy import or_, and_
import sqlalchemy.exc


warnings.filterwarnings("ignore", module='astroquery.simbad')
logger = logging.getLogger('SIMPLE')

# Logger setup
# This will stream all logger messages to the standard output and apply formatting for that
logger.propagate = False  # prevents duplicated logging messages
LOGFORMAT = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S%p')
ch = logging.StreamHandler(stream=sys.stdout)
ch.setFormatter(LOGFORMAT)
# To prevent duplicate handlers, only add if they haven't been set previously
if not len(logger.handlers):
    logger.addHandler(ch)


class SimpleError(Exception):
    pass


# TODO: commented out as not using with the new custom error
# @contextmanager
# def disable_exception_traceback():
#     """
#     All traceback information is suppressed and only the exception type and value are printed
#     """
#     default_value = getattr(sys, "tracebacklimit", 1000)  # `1000` is a Python's default value
#     sys.tracebacklimit = 0
#     yield
#     sys.tracebacklimit = default_value  # revert changes


def load_simpledb(db_file, recreatedb=True):
    # Utility function to load the database

    db_file_path = Path(db_file)
    db_connection_string = 'sqlite:///SIMPLE.db'

    if recreatedb and db_file_path.exists():
        os.remove(db_file)  # removes the current .db file if one already exists

    if not db_file_path.exists():
        create_database(db_connection_string)  # creates empty database based on the simple schema
        db = Database(db_connection_string)  # connects to the empty database
        db.load_database('data/')  # loads the data from the data files into the database
    else:
        db = Database(db_connection_string)  # if database already exists, connects to .db file

    return db


def find_source_in_db(db, source, ingest_ra=None, ingest_dec=None, search_radius=60.):
    """
    Find a source in the database given a source name and optional coordinates.

    Parameters
    ----------
    db
    source
    ingest_ra: (Optional)
        Right ascensions of sources. Decimal degrees.
    ingest_dec: (optional)
        Declinations of sources. Decimal degrees.
    search_radius
        radius in arcseconds to use for source matching
    Returns
    -------
    one match: String with database source name
    multiple matches: list of possible database names
    no matches: None

    """

    # TODO: In astrodbkit2, convert verbose to using logger

    if ingest_ra and ingest_dec:
        coords = True
    else:
        coords = False

    source = source.strip()

    logger.debug(f'Searching {source} for match in database.')

    db_name_matches = db.search_object(source, output_table='Sources', fuzzy_search=False, verbose=False)

    # NO MATCHES
    # If no matches, try fuzzy search
    if len(db_name_matches) == 0:
        db_name_matches = db.search_object(source, output_table='Sources', fuzzy_search=True, verbose=False)

    # If still no matches, try to resolve the name with Simbad
    if len(db_name_matches) == 0:
        logger.debug(f"No name matches, trying simbad search")
        db_name_matches = db.search_object(source, resolve_simbad=True, fuzzy_search=False, verbose=False)

    # if still no matches, try spatial search using coordinates, if provided
    if len(db_name_matches) == 0 and coords:
        location = SkyCoord(ingest_ra, ingest_dec, frame='icrs', unit='deg')
        radius = u.Quantity(search_radius, unit='arcsec')
        logger.info(f"No Simbad match, trying coord search around, {location.ra.hour}, {location.dec}")
        db_name_matches = db.query_region(location, radius=radius)

    if len(db_name_matches) == 1:
        db_names = db_name_matches['source'][0]
        logger.debug(f'One match found for {source}: {db_names}')
    elif len(db_name_matches) > 1:
        db_names = db_name_matches['source']
        logger.debug(f'More than match found for {source}:\n {db_names}')
        # TODO: Find way for user to choose correct match
    elif len(db_name_matches) == 0:
        db_names = None
        logger.debug(f'No match found for {source}')
    else:
        raise SimpleError(f'Unexpected condition searching for {source}')

    return db_names


def search_publication(db, name: str = None, doi: str = None, bibcode: str = None):
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
    True, 1: if only one match
    False, 0: No matches
    False, N_matches: Multiple matches

    Examples
    -------
    >>> test = search_publication(db, name='Cruz')
    Found 8 matching publications for Cruz or None or None

    >>> test = search_publication(db, name='Kirk19')
    Found 1 matching publications for Kirk19 or None or None
     name        bibcode                 doi
    ------ ------------------- ------------------------
    Kirk19 2019ApJS..240...19K 10.3847/1538-4365/aaf6af
                            description
    -----------------------------------------------------------------------------
    Preliminary Trigonometric Parallaxes of 184 Late-T and Y Dwarfs and an
    Analysis of the Field Substellar Mass Function into the Planetary Mass Regime

    >>> test = search_publication(db, name='Smith')
    No matching publications for Smith, Trying Smit
    No matching publications for Smit
    Use add_publication() to add it to the database.

    See Also
    --------
    add_publication: Function to add publications in the database

    """

    # Make sure a search term is provided
    if name is None and doi is None and bibcode is None:
        logger.error("Name, Bibcode, or DOI must be provided")
        return False, 0

    not_null_pub_filters = []
    if name:
        # fuzzy_query_name = '%' + name + '%'
        not_null_pub_filters.append(db.Publications.c.name.ilike(name))
    if doi:
        not_null_pub_filters.append(db.Publications.c.doi.ilike(doi))
    if bibcode:
        not_null_pub_filters.append(db.Publications.c.bibcode.ilike(bibcode))
    pub_search_table = Table()
    if len(not_null_pub_filters) > 0:
        pub_search_table = db.query(db.Publications).filter(or_(*not_null_pub_filters)).table()

    n_pubs_found = len(pub_search_table)

    if n_pubs_found == 1:
        logger.info(f'Found {n_pubs_found} matching publications for'
                    f' {name} or {doi} or {bibcode}')
        if logger.level <= 20:  # info
            pub_search_table.pprint_all()
        return True, 1

    if n_pubs_found > 1:
        logger.warning(f'Found {n_pubs_found} matching publications for {name} or {doi} or {bibcode}')
        if logger.level <= 30:  # warning
            pub_search_table.pprint_all()
        return False, n_pubs_found

    # If no matches found, search using first four characters of input name
    if n_pubs_found == 0 and name:
        shorter_name = name[:4]
        logger.debug(f'No matching publications for {name}, Trying {shorter_name}')
        fuzzy_query_shorter_name = '%' + shorter_name + '%'
        pub_search_table = db.query(db.Publications).filter(
            db.Publications.c.name.ilike(fuzzy_query_shorter_name)).table()
        n_pubs_found_short = len(pub_search_table)
        if n_pubs_found_short == 0:
            logger.warning(f'No matching publications for {shorter_name}')
            logger.warning('Use add_publication() to add it to the database.')
            return False, 0

        if n_pubs_found_short > 0:
            logger.warning(f'Found {n_pubs_found_short} matching publications for {shorter_name}')
            if logger.level == 20:  # info:
                pub_search_table.pprint_all()
            return False, n_pubs_found_short
    else:
        return False, n_pubs_found

    return


def add_publication(db, doi: str = None, bibcode: str = None, name: str = None, description: str = None,
                    ignore_ads: bool = False, save_db=False):
    """
    Adds publication to the database using DOI or ADS Bibcode, including metadata found with ADS.

    In order to auto-populate the fields, An $ADS_TOKEN environment variable must be set.
    See https://ui.adsabs.harvard.edu/user/settings/token

    Parameters
    ----------
    db
        Database object
    doi, bibcode: str
        The DOI or ADS Bibcode of the reference. One of these is required input.
    name: str, optional
        The publication shortname, otherwise it will be generated [optional]
        Convention is the first four letters of first authors last name and two digit year (e.g., Smit21)
        For last names which are less than four letters, use '_' or first name initial(s). (e.g, Xu__21 or LiYB21)
    description: str, optional
        Description of the paper, typically the title of the papre [optional]
    ignore_ads: bool
    save_db: bool

    See Also
    --------
    search_publication: Function to find publications in the database

    """

    if not (doi or bibcode):
        logger.error('DOI or Bibcode is required input')
        return

    ads.config.token = os.getenv('ADS_TOKEN')

    if not ads.config.token and (not name and (not doi or not bibcode)):
        logger.error("An ADS_TOKEN environment variable must be set in order to auto-populate the fields.\n"
                     "Without an ADS_TOKEN, name and bibcode or DOI must be set explicity.")
        return

    if ads.config.token and not ignore_ads:
        use_ads = True
    else:
        use_ads = False

    if bibcode:
        if 'arXiv' in bibcode:
            arxiv_id = bibcode
            bibcode = None
        else:
            arxiv_id = None
    else:
        arxiv_id = None

    name_add, bibcode_add, doi_add = '', '', ''
    # Search ADS uing a provided arxiv id
    if arxiv_id and use_ads:
        arxiv_matches = ads.SearchQuery(q=arxiv_id, fl=['id', 'bibcode', 'title', 'first_author', 'year', 'doi'])
        arxiv_matches_list = list(arxiv_matches)
        if len(arxiv_matches_list) != 1:
            logger.error('should only be one matching arxiv id')
            return

        if len(arxiv_matches_list) == 1:
            logger.info(f"Publication found in ADS using arxiv id: , {arxiv_id}")
            article = arxiv_matches_list[0]
            logger.info(f"{article.first_author}, {article.year}, {article.bibcode}, {article.title}")
            if not name:  # generate the name if it was not provided
                name_stub = article.first_author.replace(',', '').replace(' ', '')
                name_add = name_stub[0:4] + article.year[-2:]
            else:
                name_add = name
            description = article.title[0]
            bibcode_add = article.bibcode
            doi_add = article.doi[0]

    elif arxiv_id:
        name_add = name
        bibcode_add = arxiv_id
        doi_add = doi

    # Search ADS using a provided DOI
    if doi and use_ads:
        doi_matches = ads.SearchQuery(doi=doi, fl=['id', 'bibcode', 'title', 'first_author', 'year', 'doi'])
        doi_matches_list = list(doi_matches)
        if len(doi_matches_list) != 1:
            logger.error('should only be one matching DOI')
            return

        if len(doi_matches_list) == 1:
            logger.info(f"Publication found in ADS using DOI: {doi}")
            article = doi_matches_list[0]
            logger.info(f"{article.first_author}, {article.year}, {article.bibcode}, {article.title}")
            if not name:  # generate the name if it was not provided
                name_stub = article.first_author.replace(',', '').replace(' ', '')
                name_add = name_stub[0:4] + article.year[-2:]
            else:
                name_add = name
            description = article.title[0]
            bibcode_add = article.bibcode
            doi_add = article.doi[0]
    elif doi:
        name_add = name
        bibcode_add = bibcode
        doi_add = doi

    if bibcode and use_ads:
        bibcode_matches = ads.SearchQuery(bibcode=bibcode, fl=['id', 'bibcode', 'title', 'first_author', 'year', 'doi'])
        bibcode_matches_list = list(bibcode_matches)
        if len(bibcode_matches_list) == 0:
            logger.error('not a valid bibcode:' + str(bibcode))
            logger.error('nothing added')
            raise

        elif len(bibcode_matches_list) > 1:
            logger.error('should only be one matching bibcode for:' + str(bibcode))
            logger.error('nothing added')
            raise

        elif len(bibcode_matches_list) == 1:
            logger.info("Publication found in ADS using bibcode: " + str(bibcode))
            article = bibcode_matches_list[0]
            logger.info(f"{article.first_author}, {article.year}, {article.bibcode}, {article.doi}, {article.title}")
            if not name:  # generate the name if it was not provided
                name_stub = article.first_author.replace(',', '').replace(' ', '')
                name_add = name_stub[0:4] + article.year[-2:]
            else:
                name_add = name
            description = article.title[0]
            bibcode_add = article.bibcode
            if article.doi is None:
                doi_add = None
            else:
                doi_add = article.doi[0]
    elif bibcode:
        name_add = name
        bibcode_add = bibcode
        doi_add = doi

    new_ref = [{'name': name_add, 'bibcode': bibcode_add, 'doi': doi_add, 'description': description}]

    try:
        db.Publications.insert().execute(new_ref)
        logger.info(f'Added {name_add} to Publications table')
    except sqlalchemy.exc.IntegrityError:
        msg = "It's possible that a similar publication already exists in database\n" \
              "Use search_publication function before adding a new record"
        logger.error(msg)
        raise SimpleError(msg)

    if save_db:
        db.save_reference_table(table='Publications', directory='data/')
        logger.info(f"Publication added to database and saved: {name_add}")
    else:
        logger.info("Publication added to database: {name_add}")

    return


# TODO: write update_publication function
# def update_publication(db, doi: str = None, bibcode: str = None, name: str = None, description: str = None,
#                        save_db: bool = True):
#     """
#     Updates publications in the database, including metadata found with ADS.
#
#     In order to auto-populate the fields, An $ADS_TOKEN environment variable must be set.
#     See https://ui.adsabs.harvard.edu/user/settings/token
#
#     Parameters
#     ----------
#     db
#         Database object
#     doi, bibcode: str
#         The DOI or ADS Bibcode of the reference.
#     name: str, optional
#         The publication shortname, otherwise it will be generated [optional]
#     description: str, optional
#         Description of the paper, typically the title of the papre [optional]
#     save_db: bool
#
#     See Also
#     --------
#     search_publication: Function to find publications in the database
#     add_publication: Function to add publications to the database
#
#     """
#
#     # TODO: provide an option to add missing information
#     #     add_doi_bibcode = db.Publications.update().where(db.Publications.c.name == 'Manj19'). \
#     #         values(bibcode='2019AJ....157..101M', doi='10.3847/1538-3881/aaf88f',
#     #               description='Cloud Atlas: HST nir spectral library')
#     #     db.engine.execute(add_doi_bibcode)
#
#     # change_name = db.Publications.update().where(db.Publications.c.name == 'Wein12'). \
#     #         values(name='Wein13')
#     #     db.engine.execute(change_name)
#
#     return



