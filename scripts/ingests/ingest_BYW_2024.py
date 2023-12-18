from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.io import ascii
import astropy.units as u
import pandas as pd
import csv
from astropy.coordinates import Angle

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


#csv_file_path = "C:\Users\LRamon\Documents\Austin-BYW-Benchmark-Sources.-.Sheet1.csv"

#with open(csv_file_path, 'r') as file:
    # Create a CSV reader object with dictionary-like behavior
    #csv_reader = csv.DictReader(file)

    # Iterate through each row in the CSV file
    #for row in csv_reader:
        # Each 'row' variable represents a dictionary with column names as keys
     #   print(row)



# live google sheet
#sheet_id = '1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs'
#df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")
#print(df)


sheet_id = '1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs'
link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

#define the column names
columns = ['source', 'ra', 'dec', 'epoch', 'equinox', 'shortname', 'reference', 'other_ref', 'comments']
#read the csv data into an astropy table
byw_table = ascii.read(link, format='csv', data_start=2, data_end=90, header_start=1, names=columns, guess=False, delimiter=',')

data_columns = ['Source', 'RA', 'Dec', 'Epoch', 'Equinox', 'Shortname', 'Reference', 'Other_ref', 'Comments']  # columns with wanted data values

byw_table.rename_columns(data_columns)
#print result astropy table
print(byw_table)


def ingest_source_one(db):

    ingest_source_one(db, source = ["CWISE J000021.45-481314.9"], 
                  reference = "Rothermich", 
                  ra = [0.0893808], 
                  dec = [-48.2208077], 
                  epoch = [2015.4041], 
                  equinox = "ICRS")
    
db.inventory('CWISE J000021.45-481314.9', pretty_print=True)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/') 