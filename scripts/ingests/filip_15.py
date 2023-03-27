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

columns = ['Designation', 'Radius', 'log(g)', 'T_eff', 'Mass']  # columns wanted
with open("scripts/ingests/table_9_filip_15.txt") as f:
    content = f.readlines()
    table_9 = Table.read(content, format='ascii', data_start=4, data_end=201, header_start=2, delimiter='\t',
                         guess=False, fast_reader=False, include_names=columns)

data_columns = ['Radius', 'log(g)', 'T_eff', 'Mass']  # columns with data values

for i in table_9:  # rewriting so that the empty values are None
    for c in data_columns:
        if i[c] == 'cdots':
            i[c] = None


def get_float_value(value):
    # rewriting so that the string values are floats
    if value == 'None':
        return None
    else:
        float_value = float(value.split(' +or- ')[0])
        return float_value


# creating list of dictionaries for each value in table 9 formatted for modeled parameters
Radius = [{'source': i['Designation'], 'value': get_float_value(i['Radius']),
           'parameter': "radius", 'reference': "Fili15", 'unit': 'R_jup'}
          for i in table_9]
log_g = [{'source': i['Designation'], 'value': get_float_value(i['log(g)']),
          'parameter': "log g", 'reference': "Fili15", 'unit': 'dex'}
         for i in table_9]
T_eff = [{'source': i['Designation'], 'value': get_float_value(i['T_eff']),
          'parameter': "T eff", 'reference': "Fili15", 'unit': 'K'}
         for i in table_9]
Mass = [{'source': i['Designation'], 'value': get_float_value(i['Mass']),
         'parameter': "mass", 'reference': "Fili15", 'unit': 'M_jup'}
        for i in table_9]

#  getting table 1 to get proper Designation as table 9 uses a shorthand
columns_wanted = ['R.A.', 'Decl.', 'Designation']
with open("scripts/ingests/table_1_filip_15.txt") as f:
    content = f.readlines()
    table_1 = Table.read(content, format='ascii', data_start=4, data_end=201, header_start=2, delimiter='\t',
                     guess=False, fast_reader=False, include_names=columns_wanted)


def designation_to_shorthand(row_num):
    #  takes in row number for table 1 and returns the designation and shorthand
    name = table_1[row_num]['R.A.'].split(' ')[0:2] + table_1[row_num]['Decl.'].split(' ')[0:2]

    if table_1[row_num]['Designation'][-1].isalpha():
        # make sure letter designation is included
        if table_1[row_num]['Designation'][-2].isalpha():  # checks for 2 vs. 1 letter
            name = name + [table_1[row_num]['Designation'][-2:]]
        else:
            name = name + [table_1[row_num]['Designation'][-1]]

    shorthand = ('').join(name)
    designation = table_1[row_num]['Designation']
    return [shorthand, designation]


#  creating lists using above function
shorthand_and_designation = [designation_to_shorthand(i) for i in range(len(table_1))]
designations = [designation_to_shorthand(i)[1] for i in range(len(table_1))]
shorthands = [designation_to_shorthand(i)[0] for i in range(len(table_1))]


def fix_source_name(previous_name_loc, new_name):
    #  changes a sources "Designation" in each list
    Mass[previous_name_loc]['source'] = new_name
    Radius[previous_name_loc]['source'] = new_name
    log_g[previous_name_loc]['source'] = new_name
    T_eff[previous_name_loc]['source'] = new_name


def fixing_sources():
    #  finds the correct designation for each shorthand in current list
    #  works effectively as a first pass as not all will be changed
    sources_need_fixing = []
    for i in range(len(Mass)):
        name = Mass[i]['source']
        if shorthand_and_designation[i][0][:len(name)] == name:
            # many have same index in table 9 and table 1
            # matches to proper len some in table 1 have letters that table 9 don't
            fix_source_name(i, shorthand_and_designation[i][1])
        elif name in designations:
            # if true then it was previously fixed
            continue
        elif name in shorthands:
            # finds index in shorthand to replace
            loc = shorthands.index(name)
            fix_source_name(i, shorthand_and_designation[loc][1])
        else:
            sources_need_fixing.append([i, name])
    return sources_need_fixing


fixing_sources()  # runs above function


#  manually matching sources
list_verified_names = [19, 20, 24, 30, 53, 60]
for i in list_verified_names:
    fix_source_name(i, designations[i])
fix_source_name(104, designations[105])
fix_source_name(105, designations[104])
fix_source_name(134, designations[133])
fix_source_name(117, designations[118])
fix_source_name(183, designations[182])
fix_source_name(136, designations[135])
fix_source_name(153, designations[152])
fix_source_name(144, designations[143])
fix_source_name(165, designations[164])
fix_source_name(133, designations[184])
fix_source_name(173, designations[172])
fix_source_name(160, designations[159])
