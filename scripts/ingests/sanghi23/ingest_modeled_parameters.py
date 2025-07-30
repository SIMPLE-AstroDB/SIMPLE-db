from astrodb_utils import load_astrodb
from astrodbkit.schema_example import *
from astrodb_utils.publications import (
    logger,
    ingest_publication,
    find_publication
)
from astrodb_utils.sources import (
    find_source_in_db,
    AstroDBError,
    ingest_source,
    find_publication,
)
import sys
sys.path.append(".")
from simple import REFERENCE_TABLES
from astropy.io import ascii
import logging
from astropy.table import Table


logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)
SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml" 

# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

atmo_link = (
    "scripts/ingests/sanghi23/FundamentalPropertiesbyAtmoModel.csv"
)
evo_link = (
    "scripts/ingests/sanghi23/FundamentalPropertiesbyEvoModel.csv"
)

atmo_table = ascii.read(
    atmo_link, #change this to the path for the csv file
    format="csv",
    data_start=1, #starts reading data from the second line
    header_start=0, #specifies that column names are in the first line
    guess=False,
    fast_reader=False,
    delimiter=",", #specifies the character that separates datafields
)
evo_table = ascii.read(
    evo_link, #change this to the path for the csv file
    format="csv",
    data_start=1, #starts reading data from the second line
    header_start=0, #specifies that column names are in the first line
    guess=False,
    fast_reader=False,
    delimiter=",", #specifies the character that separates datafields
)

source_exists_counter = 0
skipped, reason = [], []


for row in atmo_table:
    source_name = db.search_object(row["name"])
    param_counter = 0
    while(param_counter < 4):
        if(param_counter == 1):
            parameter = "T eff"
            value = row["teff_atmo"]
            upper_error = None
            lower_error = None
            unit = "K"
        elif(param_counter == 2):
            parameter = "radius"
            value = row["radius_atmo"]
            upper_error = None
            lower_error = None
            unit = "R_jup"
        elif(param_counter == 3):
            parameter = "log g"
            value = row["logg_atmo"]
            upper_error = None
            lower_error = None
            unit = "dex"
        else:
            parameter = "L bol"
            value = row["log_lbol_lsun"]
            upper_error = row["log_lbol_lsun_err"]
            lower_error = row["log_lbol_lsun_err"]
            unit = "dex"
        param_counter += 1

        try:
            with db.engine.connect() as conn:
                conn.execute(
                    db.ModeledParameters.insert().values(
                        {
                            "source": source_name[0][0],
                            "model": "atmospheric",
                            "parameter": parameter,
                            "value": value,
                            "upper_error": upper_error,
                            "lower_error": lower_error,
                            "unit": unit,
                            "reference": "Sang23"
                        }
                    )
                )
                conn.commit()
        except IndexError:
            skipped.append(row["name"])
            reason.append("no source found, atmo model")

for row in evo_table:
    source_name = db.search_object(row["name"])
    param_counter = 0
    while(param_counter < 4):
        if(param_counter == 1):
            parameter = "T eff"
            value = row["teff_evo"]
            upper_error = row["teff_evo_err"]
            lower_error = row["teff_evo_err"]
            unit = "K"
        elif(param_counter == 2):
            parameter = "radius"
            value = row["radius_evo"]
            upper_error = row["radius_evo_err"]
            lower_error = row["radius_evo_err"]
            unit = "R_jup"
        elif(param_counter == 3):
            parameter = "mass"
            value = row["mass_evo"]
            upper_error = row["mass_evo_err"]
            lower_error = row["mass_evo_err"]
            unit = "M_jup"
        else:
            parameter = "log g"
            value = row["logg_evo"]
            upper_error = row["logg_evo_err"]
            lower_error = row["logg_evo_err"]
            unit = "dex"
        param_counter += 1

        try:
            with db.engine.connect() as conn:
                conn.execute(
                    db.ModeledParameters.insert().values(
                        {
                            "source": source_name[0][0],
                            "model": "evolutionary",
                            "parameter": parameter,
                            "value": value,
                            "upper_error": upper_error,
                            "lower_error": lower_error,
                            "unit": unit,
                            "reference": "Sang23"
                        }
                    )
                )
                conn.commit()
        except IndexError:
            skipped.append(row["name"])
            reason.append("no source found, evo model")

logger.info("done")

skipped_table = Table([skipped, reason], names=["Skipped", "Reason"])
skipped_table.write(
    "scripts/ingests/sanghi23/sanghi23_modeled_parameters_skipped.csv",
    delimiter=",",
    overwrite=True,
    format="ascii.ecsv",
)

if SAVE_DB:
    db.save_database(directory="data/")

    