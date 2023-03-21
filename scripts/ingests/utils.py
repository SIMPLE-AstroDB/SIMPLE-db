"""
Utils functions for use in ingests
"""
import logging
import os
import sys
import re
import warnings
from pathlib import Path
from astrodbkit2.astrodb import create_database, Database
from simple.schema import *
import ads
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.table import Table, unique
from sqlalchemy import or_, and_
import sqlalchemy.exc
from astroquery.simbad import Simbad
from astropy.coordinates import SkyCoord
import astropy.units as u
import socket
from scripts import REFERENCE_TABLES

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
logger.setLevel(logging.INFO)
    

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


def load_simpledb(db_file, recreatedb=True, reference_tables=REFERENCE_TABLES):
    # Utility function to load the database

    db_file_path = Path(db_file)
    db_connection_string = 'sqlite:///' + db_file

    if recreatedb and db_file_path.exists():
        os.remove(db_file)  # removes the current .db file if one already exists

    if not db_file_path.exists():
        try: # Use fancy in-memory database, if supported by astrodbkit2
            db = Database('sqlite://', reference_tables=REFERENCE_TABLES)  # creates and connects to a temporary in-memory database
            db.load_database('data/')  # loads the data from the data files into the database
            db.dump_sqlite(db_file)  # dump in-memory database to file
            db = Database(db_connection_string, reference_tables=REFERENCE_TABLES)  # replace database object with new file version
        except RuntimeError:
            # use in-file database
            create_database(db_connection_string)  # creates empty database based on the simple schema
            db = Database(db_connection_string, reference_tables=REFERENCE_TABLES)  # connects to the empty database
            db.load_database('data/')  # loads the data from the data files into the database
    else:
        db = Database(db_connection_string, reference_tables=REFERENCE_TABLES)  # if database already exists, connects to .db file

    return db


def find_source_in_db(db, source, ra=None, dec=None, search_radius=60.):
    """
    Find a source in the database given a source name and optional coordinates.

    Parameters
    ----------
    db
    source: str
        Source name
    ra: float
        Right ascensions of sources. Decimal degrees.
    dec: float
        Declinations of sources. Decimal degrees.
    search_radius
        radius in arcseconds to use for source matching

    Returns
    -------
    List of strings.

    one match: Single element list with one database source name
    multiple matches: List of possible database names
    no matches: Empty list

    """

    # TODO: In astrodbkit2, convert verbose to using logger

    if ra and dec:
        coords = True
    else:
        coords = False

    source = source.strip()

    logger.debug(f'{source}: Searching for match in database.')

    db_name_matches = db.search_object(source, output_table='Sources', fuzzy_search=False, verbose=False)

    # NO MATCHES
    # If no matches, try fuzzy search
    if len(db_name_matches) == 0:
        logger.debug(f"{source}: No name matches, trying fuzzy search")
        db_name_matches = db.search_object(source, output_table='Sources', fuzzy_search=True, verbose=False)

    # If still no matches, try to resolve the name with Simbad
    if len(db_name_matches) == 0:
        logger.debug(f"{source}: No name matches, trying Simbad search")
        db_name_matches = db.search_object(source, resolve_simbad=True, fuzzy_search=False, verbose=False)

    # if still no matches, try spatial search using coordinates, if provided
    if len(db_name_matches) == 0 and coords:
        location = SkyCoord(ra, dec, frame='icrs', unit='deg')
        radius = u.Quantity(search_radius, unit='arcsec')
        logger.info(f"{source}: No Simbad match, trying coord search around {location.ra.degree}, {location.dec}")
        db_name_matches = db.query_region(location, radius=radius)

    # If still no matches, try to get the coords from SIMBAD
    if len(db_name_matches) == 0:
        simbad_result_table = Simbad.query_object(source)
        if simbad_result_table is not None and len(simbad_result_table) == 1:
            simbad_coords = simbad_result_table['RA'][0] + ' ' + simbad_result_table['DEC'][0]
            simbad_skycoord = SkyCoord(simbad_coords, unit=(u.hourangle, u.deg))
            ra = simbad_skycoord.to_string(style='decimal').split()[0]
            dec = simbad_skycoord.to_string(style='decimal').split()[1]
            msg = f"Coordinates retrieved from SIMBAD {ra}, {dec}"
            logger.debug(msg)
            # Search database around that coordinate
            radius = u.Quantity(search_radius, unit='arcsec')
            db_name_matches = db.query_region(simbad_skycoord, radius=radius)

    if len(db_name_matches) == 1:
        db_names = db_name_matches['source'].tolist()
        logger.debug(f'One match found for {source}: {db_names[0]}')
    elif len(db_name_matches) > 1:
        db_names = db_name_matches['source'].tolist()
        logger.debug(f'More than match found for {source}: {db_names}')
        # TODO: Find way for user to choose correct match
    elif len(db_name_matches) == 0:
        db_names = []
        logger.debug(f' {source}: No match found')
    else:
        raise SimpleError(f'Unexpected condition searching for {source}')

    return db_names


def find_publication(db, name: str = None, doi: str = None, bibcode: str = None):
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
    True, str: if only one match
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
        not_null_pub_filters.append(db.Publications.c.reference.ilike(name))
    if doi:
        not_null_pub_filters.append(db.Publications.c.doi.ilike(doi))
    if bibcode:
        not_null_pub_filters.append(db.Publications.c.bibcode.ilike(bibcode))
    pub_search_table = Table()
    if len(not_null_pub_filters) > 0:
        pub_search_table = db.query(db.Publications).filter(or_(*not_null_pub_filters)).table()

    n_pubs_found = len(pub_search_table)

    if n_pubs_found == 1:
        logger.info(f'Found {n_pubs_found} matching publications for '
                    f"{name} or {doi} or {bibcode}: {pub_search_table['reference'].data}")
        if logger.level <= 10:  # debug
            pub_search_table.pprint_all()
        return True, pub_search_table['reference'].data[0]

    if n_pubs_found > 1:
        logger.warning(f'Found {n_pubs_found} matching publications for {name} or {doi} or {bibcode}')
        if logger.level <= 30:  # warning
            pub_search_table.pprint_all()
        return False, n_pubs_found

    # If no matches found, search using first four characters of input name
    if n_pubs_found == 0 and name:
        shorter_name = name[:4]
        logger.debug(f'No matching publications for {name}, Trying {shorter_name}.')
        fuzzy_query_shorter_name = '%' + shorter_name + '%'
        pub_search_table = db.query(db.Publications).filter(
            db.Publications.c.reference.ilike(fuzzy_query_shorter_name)).table()
        n_pubs_found_short = len(pub_search_table)
        if n_pubs_found_short == 0:
            logger.warning(f'No matching publications for {name} or {shorter_name}')
            logger.warning('Use add_publication() to add it to the database.')
            return False, 0

        if n_pubs_found_short > 0:
            logger.debug(f'Found {n_pubs_found_short} matching publications for {shorter_name}')
            if logger.level == 10:  # debug
                pub_search_table.pprint_all()

            #  Try to find numbers in the reference which might be a date
            dates = re.findall(r'\d+', name)
            # try to find a two digit date
            if len(dates) == 0:
                logger.debug(f'Could not find a date in {name}')
                two_digit_date = None
            elif len(dates) == 1:
                if len(dates[0]) == 4:
                    two_digit_date = dates[0][2:]
                elif len(dates[0]) == 2:
                    two_digit_date = dates[0]
                else:
                    logger.debug(f'Could not find a two digit date using {dates}')
                    two_digit_date = None
            else:
                logger.debug(f'Could not find a two digit date using {dates}')
                two_digit_date = None

            if two_digit_date:
                logger.debug(f'Trying to limit using {two_digit_date}')
                n_pubs_found_short_date = 0
                pubs_found_short_date = []
                for pub in pub_search_table['reference']:
                    if pub.find(two_digit_date) != -1:
                        n_pubs_found_short_date += 1
                        pubs_found_short_date.append(pub)
                if n_pubs_found_short_date == 1:
                    logger.debug(f'Found {n_pubs_found_short_date} matching publications for '
                                f'{name} using {shorter_name} and {two_digit_date}')
                    logger.debug(f'{pubs_found_short_date}')
                    return True, pubs_found_short_date[0]
                else:
                    logger.warning(f'Found {n_pubs_found_short_date} matching publications for '
                                   f'{name} using {shorter_name} and {two_digit_date}')
                    logger.warning(f'{pubs_found_short_date}')
                    return False, n_pubs_found_short_date
            else:
                return False, n_pubs_found_short
    else:
        return False, n_pubs_found

    return


def ingest_publication(db, doi: str = None, bibcode: str = None, publication: str = None, description: str = None,
                    ignore_ads: bool = False):
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
    publication: str, optional
        The publication shortname, otherwise it will be generated [optional]
        Convention is the first four letters of first authors last name and two digit year (e.g., Smit21)
        For last names which are less than four letters, use '_' or first name initial(s). (e.g, Xu__21 or LiYB21)
    description: str, optional
        Description of the paper, typically the title of the papre [optional]
    ignore_ads: bool

    See Also
    --------
    search_publication: Function to find publications in the database

    """

    if not (publication or doi or bibcode):
        logger.error('Publication, DOI, or Bibcode is required input')
        return

    ads.config.token = os.getenv('ADS_TOKEN')

    if not ads.config.token and (not publication and (not doi or not bibcode)):
        logger.error("An ADS_TOKEN environment variable must be set in order to auto-populate the fields.\n"
                     "Without an ADS_TOKEN, name and bibcode or DOI must be set explicity.")
        return

    if ads.config.token and not ignore_ads:
        use_ads = True
    else:
        use_ads = False
    logger.debug(f"Use ADS set to {use_ads}")

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
            logger.debug(f"Publication found in ADS using arxiv id: , {arxiv_id}")
            article = arxiv_matches_list[0]
            logger.debug(f"{article.first_author}, {article.year}, {article.bibcode}, {article.title}")
            if not publication:  # generate the name if it was not provided
                name_stub = article.first_author.replace(',', '').replace(' ', '')
                name_add = name_stub[0:4] + article.year[-2:]
            else:
                name_add = publication
            description = article.title[0]
            bibcode_add = article.bibcode
            doi_add = article.doi[0]

    elif arxiv_id:
        name_add = publication
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
            logger.debug(f"Publication found in ADS using DOI: {doi}")
            using = doi
            article = doi_matches_list[0]
            logger.debug(f"{article.first_author}, {article.year}, {article.bibcode}, {article.title}")
            if not publication:  # generate the name if it was not provided
                name_stub = article.first_author.replace(',', '').replace(' ', '')
                name_add = name_stub[0:4] + article.year[-2:]
            else:
                name_add = publication
            description = article.title[0]
            bibcode_add = article.bibcode
            doi_add = article.doi[0]
    elif doi:
        name_add = publication
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
            logger.debug("Publication found in ADS using bibcode: " + str(bibcode))
            using = str(bibcode)
            article = bibcode_matches_list[0]
            logger.debug(f"{article.first_author}, {article.year}, {article.bibcode}, {article.doi}, {article.title}")
            if not publication:  # generate the name if it was not provided
                name_stub = article.first_author.replace(',', '').replace(' ', '')
                name_add = name_stub[0:4] + article.year[-2:]
            else:
                name_add = publication
            description = article.title[0]
            bibcode_add = article.bibcode
            if article.doi is None:
                doi_add = None
            else:
                doi_add = article.doi[0]
    elif bibcode:
        name_add = publication
        bibcode_add = bibcode
        doi_add = doi

    if publication and not bibcode and not doi:
        name_add = publication
        using = 'user input'

    new_ref = [{'reference': name_add, 'bibcode': bibcode_add, 'doi': doi_add, 'description': description}]

    try:
        with db.engine.connect() as conn:
            conn.execute(db.Publications.insert().values(new_ref))
            conn.commit()
        logger.info(f'Added {name_add} to Publications table using {using}')
    except sqlalchemy.exc.IntegrityError as error:
        msg = f"Not able to add {new_ref} to the database. " \
              f"It's possible that a similar publication already exists in database\n"\
              "Use find_publication function before adding a new record"
        logger.error(msg)
        raise SimpleError(msg + str(error))

    return



def check_internet_connection():
    # get current IP address of  system
    ipaddress = socket.gethostbyname(socket.gethostname())

    # checking system IP is the same as "127.0.0.1" or not.
    if ipaddress == "127.0.0.1": # no internet
        return False, ipaddress
    else:
        return True, ipaddress

