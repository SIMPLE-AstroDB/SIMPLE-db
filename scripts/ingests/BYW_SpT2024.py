from astrodb_utils import load_astrodb
from simple.schema import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.db", recreatedb=RECREATE_DB)

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
def ingest_all_source_spt(db):
    for row in byw_table[1:90]:  # skip the header row - [1:10]runs only first 10 rows
        # Print byw source information
        print("BYW Source SpT Information:")


        for col_name in row.colnames:
            print(f"{col_name}: {row[col_name]}")

        ingest_spectral_types(
            db,
            source=row["Source"],
            spectral_type_string=row["spectral_type_string"],
            spectral_type_code=row["spectral_type_code"],
            spectral_type_error=row["spectral_type_error"],
            regime=row["regime"],
            adopted=row["adopted"],
            comments=row["comments"],
            reference=row["Reference"]
            raise_error=True,
            search_db=True,
        )

        # Add a separator between rows for better readability
        print("-" * 20)    