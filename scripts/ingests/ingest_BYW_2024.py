from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.io import ascii

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb("SIMPLE.db", recreatedb=RECREATE_DB)

# Load Google sheet
sheet_id = "1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs"
link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

# read the csv data into an astropy table
# ascii.read attempts to read data from local files rather from URLs so using a library like requests helps get data and create object that can be passed to ascii.read
byw_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

# print result astropy table
print(byw_table.info)


# Loop through each row in byw table and print data: source name ra, dec.
def ingest_all_sources(db):
    for row in byw_table[1:90]:  # skip the header row - [1:10]runs only first 10 rows
        # Print byw source information
        print("BYW Source Information:")
        # for key, value in row_dict.items():
        # print(f"{key}: {value}")

        for col_name in row.colnames:
            print(f"{col_name}: {row[col_name]}")

        ingest_source(
            db,
            source=row["Source"],
            reference=row["Reference"],
            ra=row["RA"],
            dec=row["Dec"],
            epoch=row["Epoch"],
            equinox=row["Equinox"],
            raise_error=True,
            search_db=True,
        )
        # print(row['ra'])
        # print(row_dict['ra'])

        # Add a separator between rows for better readability
        print("-" * 20)

#Call sources function
ingest_all_sources(db)


#Ingest shortnames as other names to source
#Loop through data 
def ingest_all_shortnames(db):
    for row in byw_table[55:]:  # skip the header row - [1:10]runs only first 10 rows

        # Print byw source information
        print("BYW Source Information:")
        
        for col_name in row.colnames:
            print(f"{col_name}: {row[col_name]}")

        ingest_names(db, 
                 source=row["Source"], 
                 other_name=row["Shortname"]
        )

        print("-" * 20)

#Call shortnames function
ingest_all_shortnames(db)


# Ingested other_ref sources as publications
# Skrzypek et al. 2016, Marocco et al. 2015(Online Catalog), Kirkpatrick et al. 2021
# Ingest reference name: Skrz16, Maro15, Kirk21
def add_publication(db):
    ingest_publication(
        db, doi="10.26093/cds/vizier.35890049", bibcode="2016yCat..35890049S"
    )

    ingest_publication(db, doi="10.1093/mnras/stv530", bibcode="2015MNRAS.449.3651M")

    ingest_publication(
        db, doi="10.3847/1538-4365/abd107", bibcode="2021ApJS..253....7K"
    )

#Call publications function
# add_publication(db)

def fix_blobs(db):
    with db.engine.begin() as conn:
        conn.execute(
            db.Sources.update()
            .where(db.Sources.c.reference == "Roth")
            .values(other_references=None, comments=None)
        )


def delete_roth_sources(db):
    with db.engine.begin() as conn:
        conn.execute(db.Sources.delete().where(db.Sources.c.reference == "Roth"))




db.inventory("CWISE J000021.45-481314.9", pretty_print=True)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")
