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
SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml" 

# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

L6T6_link = (
    "scripts/ingests/ultracool_sheet/L6_to_T6_benchmarks08062025.csv"
)
T7Y1_link = (
    "scripts/ingests/ultracool_sheet/T7_to_Y1_benchmarks08062025.csv"
)

L6T6_table = ascii.read(
    L6T6_link, #change this to the path for the csv file
    format="csv",
    data_start=1, #starts reading data from the second line
    header_start=0, #specifies that column names are in the first line
    guess=False,
    fast_reader=False,
    delimiter=",", #specifies the character that separates datafields
)
T7Y1_table = ascii.read(
    T7Y1_link, #change this to the path for the csv file
    format="csv",
    data_start=1, #starts reading data from the second line
    header_start=0, #specifies that column names are in the first line
    guess=False,
    fast_reader=False,
    delimiter=",", #specifies the character that separates datafields
)

teff_ingested_counter, logg_ingested_counter, mass_ingested_counter, radius_ingested_counter, lbol_ingested_counter = 0, 0, 0, 0, 0
skipped, reason = [], []


def get_pub(ads):

    pub = find_publication(db = db, bibcode = ads)
    if(pub[0] == False):
        ingest_publication(db = db, bibcode = ads)
    return find_publication(db = db, bibcode = ads)[1]

for row in L6T6_table:
    source_name = db.search_object(row["object"])
    param_counter = 0
    while(param_counter < 4):
        if(param_counter == 1):
            parameter = "L bol"
            value = row["log_Lbol"]
            upper_error = row["log_Lbol_upp_err"]
            lower_error = row["log_Lbol_low_err"]
            unit = "dex"
            ref = get_pub(row["Ref_lbol"])
        elif(param_counter == 2):
            parameter = "T eff"
            value = row["Teff"]
            upper_error = row["Teff_upp_err"]
            lower_error = row["Teff_low_err"]
            unit = "K"
            ref = get_pub(row["Ref_TeffloggRadiusMass"])
        elif(param_counter == 3):
            parameter = "log g"
            value = row["logg"]
            upper_error = row["logg_upp_err"]
            lower_error = row["logg_low_err"]
            unit = "dex"
            ref = get_pub(row["Ref_TeffloggRadiusMass"])
        else:
            parameter = "mass"
            value = row["Mass_MJ"]
            upper_error = row["Mass_MJ_upp_err"]
            lower_error = row["Mass_MJ_low_err"]
            unit = "M_jup"
            ref = get_pub(row["Ref_TeffloggRadiusMass"])
        param_counter += 1

        try:
            with db.engine.connect() as conn:
                conn.execute(
                    db.ModeledParameters.insert().values(
                        {
                            "source": source_name[0][0],
                            "model": None,
                            "parameter": parameter,
                            "value": value,
                            "upper_error": upper_error,
                            "lower_error": lower_error,
                            "unit": unit,
                            "reference": ref
                        }
                    )
                )
                conn.commit()
            if(param_counter == 1):
                lbol_ingested_counter += 1
            elif(param_counter == 2):
                teff_ingested_counter += 1
            elif(param_counter == 3):
                logg_ingested_counter += 1
            else:
                mass_ingested_counter += 1
        except IndexError:
            skipped.append(row["object"])
            reason.append("no source found, L6T6")

for row in T7Y1_table:
    source_name = db.search_object(row["object"])
    param_counter = 0
    while(param_counter < 5):
        if(param_counter == 1):
            parameter = "L bol"
            value = row["log_Lbol"]
            upper_error = row["log_Lbol_upp_err"]
            lower_error = row["log_Lbol_low_err"]
            unit = "dex"
            ref = get_pub(row["Ref_lbol"])
        elif(param_counter == 2):
            parameter = "T eff"
            value = row["Teff"]
            upper_error = row["Teff_upp_err"]
            lower_error = row["Teff_low_err"]
            unit = "K"
            ref = get_pub(row["Ref_TeffloggRadiusMass"])
        elif(param_counter == 3):
            parameter = "log g"
            value = row["logg"]
            upper_error = row["logg_upp_err"]
            lower_error = row["logg_low_err"]
            unit = "dex"
            ref = get_pub(row["Ref_TeffloggRadiusMass"])
        elif(param_counter == 4):
            parameter = "radius"
            value = row["Radius_RJ"]
            upper_error = row["Radius_RJ_upp_err"]
            lower_error = row["Radius_RJ_low_err"]
            unit = "R_jup"
            ref = get_pub(row["Ref_TeffloggRadiusMass"])
        else:
            parameter = "mass"
            value = row["Mass_MJ"]
            upper_error = row["Mass_MJ_upp_err"]
            lower_error = row["Mass_MJ_low_err"]
            unit = "M_jup"
            ref = get_pub(row["Ref_TeffloggRadiusMass"])
        param_counter += 1

        try:
            with db.engine.connect() as conn:
                conn.execute(
                    db.ModeledParameters.insert().values(
                        {
                            "source": source_name[0][0],
                            "model": None,
                            "parameter": parameter,
                            "value": value,
                            "upper_error": upper_error,
                            "lower_error": lower_error,
                            "unit": unit,
                            "reference": ref
                        }
                    )
                )
                conn.commit()
            if(param_counter == 1):
                lbol_ingested_counter += 1
            elif(param_counter == 2):
                teff_ingested_counter += 1
            elif(param_counter == 3):
                logg_ingested_counter += 1
            elif(param_counter == 4):
                radius_ingested_counter += 1
            else:
                mass_ingested_counter += 1
        except IndexError:
            skipped.append(row["object"])
            reason.append("no source found, T7Y1")

logger.info("done")

skipped_table = Table([skipped, reason], names=["Skipped", "Reason"])
skipped_table.write(
    "scripts/ingests/ultracool_sheet/uc_modeled_parameters_skipped.csv",
    delimiter=",",
    overwrite=True,
    format="ascii.ecsv",
)



if SAVE_DB:
    db.save_database(directory="data/")

print("teff ingested: " + str(teff_ingested_counter)) #55
print("mass ingested: " + str(mass_ingested_counter)) #55
print("radius ingested: " + str(radius_ingested_counter)) #14
print("logg ingested: " + str(logg_ingested_counter)) #55
print("lbol ingested: " + str(lbol_ingested_counter)) #55

