from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.io import ascii
import astropy.units as u
import requests
from io import StringIO
from astropy.coordinates import Angle

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

#Attempt 1
#csv_file_path = "C:\Users\LRamon\Documents\Austin-BYW-Benchmark-Sources.-.Sheet1.csv"

#with open(csv_file_path, 'r') as file:
    # Create a CSV reader object with dictionary-like behavior
    #csv_reader = csv.DictReader(file)

    # Iterate through each row in the CSV file
    #for row in csv_reader:
        # Each 'row' variable represents a dictionary with column names as keys
     #   print(row)



# live google sheet (Attempt 2)
#sheet_id = '1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs'
#df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")
#print(df)


#(Attempt 3)
sheet_id = '1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs'
link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

# Fetch the content from the URL
response = requests.get(link)
data = response.text

# Use io.StringIO to create a file-like object
data_file = StringIO(data)

#define the column names
columns = ['source', 'ra', 'dec', 'epoch', 'equinox', 'shortname', 'reference', 'other_ref', 'comments']

#read the csv data into an astropy table
#ascii.read attempts to read data from local files rather from URLs so using a library like requests helps get data and create object that can be passed to ascii.read
byw_table = ascii.read(link, format='csv', data_start=1, data_end=90, header_start=1, names=columns, guess=False, fast_reader=False, delimiter=',')

#data_columns = ['Source', 'RA', 'Dec', 'Epoch', 'Equinox', 'Shortname', 'Reference', 'Other_ref', 'Comments']  # columns with wanted data values
#byw_table.rename_columns(data_columns)

#print result astropy table
print(byw_table)

#Loop over the byw table and print source name ra, dec, at each row. 
#Change all references to Roth; search and replace 
#Individually ingest other_ref sources as publications, then use those 3 to modify the 3 sources to be ingested in the loop


#Individual source ingest attempt...
def ingest_source_one(db):

    ingest_source(db, source = "CWISE J000021.45-481314.9", 
                  reference = "Roth", 
                  ra = 0.0893808, 
                  dec = -48.2208077, 
                  epoch = 2015.4041, 
                  equinox = "ICRS", 
                  raise_error=True, 
                  search_db=True, 
                  other_reference=None, 
                  comment=None)
    
    ingest_source(db, source = "CWISE J002029.72-153527.5", 
                  reference = "Roth", 
                  ra = 5.1238388, 
                  dec = -15.5909815, 
                  epoch = 2015.4041, 
                  equinox = "ICRS", 
                  raise_error=True, 
                  search_db=True, 
                  other_reference=None, 
                  comment=None)


#All sources ingest attempt...
#def ingest_all_sources(db):
#    ingest_sources(db, sources = [" "], 
#                   references=None, 
#                   ras=None, 
#                   decs=None, 
#                   comments=None, 
#                   epochs=None,
#                   equinoxes=None, 
#                   other_references=None, 
#                   raise_error=True, 
#                   search_db=True)   

#calling functions    
ingest_source_one(db)    
OR
#ingest_all_sources(db)    
    
db.inventory('CWISE J000021.45-481314.9', pretty_print=True)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/') 