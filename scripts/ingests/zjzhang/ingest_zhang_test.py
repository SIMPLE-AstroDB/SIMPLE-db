from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    find_source_in_db,
    AstroDBError,
    find_publication,
)

from astrodb_utils.publications import (
    ingest_publication,
    logger,
)

from sigfig import round

import sys

sys.path.append(".")
from astropy.io import ascii
from simple import REFERENCE_TABLES


DB_SAVE = False
RECREATE_DB = True
SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH
)


def add_publications(db):
    """
    Add publications to the database.
    """
    # add publication to the database

    # ingest doi publications because it isn't compatible with extractADS function
    # comment out when DB_SAVE = True because publications and sources already exist in db
    ingest_publication(
        db, doi="10.1088/0004-637X/748/2/93", reference="Roja12"
    )  # Roja12
    # ingest_publication(db, doi = "10.1088/0067-0049/203/2/21", reference = "Ahn_12") #ahn_12
    ingest_publication(db, bibcode="2018MNRAS.479.1383Z", reference="Zhan18.1352")
    ingest_publication(db, bibcode="2018MNRAS.480.5447Z", reference="Zhan18.2054")

"""
ingest_source(
    db,
    "ULAS J074431.30+283915.6",
    search_db=True,
    reference="Ahn_12",
    ra_col_name="ra",
    dec_col_name="dec",
    epoch_col_name="epoch",
)

ingest_source(
    db,
    "LHS 292",
    search_db=False,
    reference="Roja12",
    ra_col_name="ra",
    dec_col_name="dec",
    epoch_col_name="epoch",
)
"""
counter = 0
# modeled parameters
#columbs: source, model, parameter, value, upper_error, lower_error, unit, comments, reference

# Load photometry sheet
link = "scripts/ingests/zjzhang/L6_to_T6_benchmark.csv"

# read the csv data into an astropy table
zhang_sheet_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

def two_decimal(var):
    return round(var, decimals=2)

print(two_decimal(0.123456789)) #test the function

parameter = 0 #set parameter to 0 at the start of the loop
for row in zhang_sheet_table:
    parameter = 0 #reset parameter to 0 for each new source
    source = db.search_object(row["object"]) #search for source in db --> returns a list / if not found returns empty list 
    while(parameter < 4): #Loop over the 4 parameters for each source
        if(parameter == 0): #Parameter 1 log_Lbol
            value = row["log_Lbol"]
            upper_error = row["log_Lbol_upp_err"]
            lower_error = row["log_Lbol_low_err"]
            ref = row["Ref_lbol"]
            unit = "dex"
        elif(parameter == 1): #Parameter 2 Teff
            value = row["Teff"]
            upper_error = row["Teff_upp_err"]
            lower_error = row["Teff_low_err"]
            ref = row["Ref_TeffloggRadiusMass"]
            unit = "K"
        elif(parameter == 2): #Parameter 3 log_g
            value = row["logg"]
            upper_error = row["logg_upp_err"]
            lower_error = row["logg_low_err"]
            ref = row["Ref_TeffloggRadiusMass"]
            unit = "dex"
        elif(parameter == 3): #Parameter 4 Mass
            value = row["Mass_MJ"]
            upper_error = row["Mass_MJ_upp_err"]
            lower_error = row["Mass_MJ_low_err"]
            ref = row["Ref_TeffloggRadiusMass"]
            unit = "M_jup"
        print("Source: " + str(source))
        print("Parameter: " + str(parameter))
        print("Value: " + value)
        print("Upper Error: " + upper_error)
        print("Lower Error: " + lower_error)
        print("Reference: " + ref)
        print("Unit: " + unit)
        """
        try:
            with db.engine.connect() as conn: #Opens a connection to the database
                conn.execute(
                    db.ModeledParameters.insert().values(
                        #Tries to ingest the values into the modeled parameters table
                        {
                            "source": source,
                            "model": None,
                            "parameter": parameter,
                            "value": two_decimal(value),
                            "upper_error": two_decimal(upper_error),
                            "lower_error": two_decimal(lower_error),
                            "unit": unit,
                            "comments": None,
                            "reference": ref
                        }
                    )
                )
                conn.commit()
        except IndexError as a: #If source is not found in the db, it will skip the ingestion of the parameters and move on to the next source
            counter = counter + 1
        """
        parameter += 1