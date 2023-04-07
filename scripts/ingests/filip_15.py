# script to get the data from table 9 of Filippazzo 2015 paper for modeled parameters

from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *

from astropy.table import Table

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
# live google sheet
link = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTJZccUnKGOLcfPixaS3Tib0sqqC23n1Q_0ybjwXN4ZDYBT_-2_FeHLShVBzkIDfo39-tV1aoz5rCth/pub?output=csv"

columns = ['Designation 9', 'pi', 'pi References', 'log(L_*/L_sun)', 'Age^a', 'Radius', 'log(g)', 'T_eff', 'Mass',
           'R.A.', 'Decl.', 'Designation 1', 'Discovery References', 'SpT', 'SpT References']
data9stuff = Table.read(link, format='ascii', data_start=6, data_end=203, header_start=4, names=columns, guess=False,
                        fast_reader=False, delimiter=',')

data_columns = ['Radius', 'log(g)', 'T_eff', 'Mass']  # columns with wanted data values

for row in data9stuff:  # rewriting so that the empty values are None
    for column in data_columns:
        if row[column] == 'cdots':
            row[column] = None


def get_float_value(value, error=False):
    # rewriting so that the string values are floats
    # if error == True returns uncertainty value instead of
    if value == 'None':
        return None

    elif error:
        # returns uncertainty value
        float_error_value = float(value.split(' +or- ')[1])
        return float_error_value

    else:
        # returns value
        float_value = float(value.split(' +or- ')[0])
        return float_value


# creating list of dictionaries for each value in table 9 formatted for modeled parameters
Radius = [{'source': row['Designation 1'], 'value': get_float_value(row['Radius']),
           'value_error': get_float_value(row['Radius'], error = True),
           'parameter': "radius", 'reference': "Fili15", 'unit': 'R_jup'}
          for row in data9stuff]
log_g = [{'source': row['Designation 1'], 'value': get_float_value(row['log(g)']),
          'value_error': get_float_value(row['log(g)'], error = True),
          'parameter': "log g", 'reference': "Fili15", 'unit': 'dex'}
         for row in data9stuff]
T_eff = [{'source': row['Designation 1'], 'value': get_float_value(row['T_eff']),
          'value_error': get_float_value(row['T_eff'], error = True),
          'parameter': "T eff", 'reference': "Fili15", 'unit': 'K'}
         for row in data9stuff]
Mass = [{'source': row['Designation 1'], 'value': get_float_value(i['Mass']),
         'value_error': get_float_value(row['Mass'], error = True),
         'parameter': "mass", 'reference': "Fili15", 'unit': 'M_jup'}
        for row in data9stuff]
