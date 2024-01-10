from scripts.ingests.utils import load_simpledb, find_source_in_db, logger, SimpleError
from astropy.io import ascii
from urllib.parse import quote
import requests
from astropy.table import Table


RECREATE_DB = False
db = load_simpledb("SIMPLE.sqlite", recreatedb=RECREATE_DB)

# Load Ultracool sheet
sheet_id = "1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8"
# link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
link = "scripts/ultracool_sheet/UltracoolSheet - Main_010824.csv"

# read the csv data into an astropy table
uc_sheet_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

# Match sources in Ultracool sheet to sources in SIMPLE
uc_names = []
simple_urls = []
simple_sources = []
for source in uc_sheet_table[2368:]:
    uc_sheet_name = source["name"]
    match = find_source_in_db(
        db,
        uc_sheet_name,
        ra=source["ra_j2000_formula"],
        dec=source["dec_j2000_formula"],
    )

    # convert SIMPLE source name to URL
    if len(match) == 0:
        msg = f"No match found for {uc_sheet_name}"
        logger.error(msg)
        raise SimpleError(msg)
    elif len(match) > 1:
        msg = f"Multiple matches found for {uc_sheet_name}"
        logger.error(msg)
        raise SimpleError(msg)
    elif len(match) == 1:
        simple_source = match[0]
        logger.info(f"Match found for {uc_sheet_name}: {simple_source}")
    else:
        msg = f"Unexpected state for {uc_sheet_name}"
        logger.error(msg)
        raise SimpleError(msg)

    # URLify source name
    source_url = quote(simple_source)
    url = "https://simple-bd-archive.org/solo_result/" + source_url

    # test the URL to make sure it is valid
    url_status = requests.head(url).status_code
    if url_status == 404:
        msg = f"URL not valid for {uc_sheet_name} {simple_source} at {url}"
        logger.error(msg)  
        raise SimpleError(msg)  
    elif url_status != 200:
        logger.warning(f"URL not valid for {uc_sheet_name} {simple_source} at {url} 
                       but with HTTP status {url_status}")
    else:
        logger.info(f"URL valid for {uc_sheet_name} {simple_source} at {url}")

    #  ('URL not valid for ', 'AB Pic b', 'HD 44627B', 'https://simple-bd-archive.org/solo_result/HD%2044627B')
    #  ('URL not valid for ', '2MASSI J1707333+430130', '2MASS J17073334+4301304', 'https://simple-bd-archive.org/solo_result/2MASS%20J17073334%2B4301304')

    uc_names.append(uc_sheet_name)
    simple_sources.append(simple_source)
    simple_urls.append(url)

# write the results to a file
results_table = Table(
    [uc_names, simple_sources, simple_urls],
    names=["Ultracool Sheet Name", "SIMPLE Source Name", "SIMPLE URL"],
)
results_table.write(
    "scripts/ultracool_sheet/uc_sheet_simple_urls.csv",
    delimiter=",",
    overwrite=True,
    format="ascii.ecsv",
)
