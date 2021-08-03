import sys
sys.path.append('.')
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from astropy.table import Table
import numpy as np
from scripts.ingests.utils import * 
from astropy.coordinates import SkyCoord
import astropy.units as u
from astroquery.simbad import Simbad
import warnings
warnings.filterwarnings("ignore", module='astroquery.simbad')
import re
import os
from pathlib import Path
import pandas as pd


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False

verboseprint = print if VERBOSE else lambda *a, **k: None

def load_db():
    # Utility function to load the database

    db_file = 'SIMPLE.db'
    db_file_path = Path(db_file)
    db_connection_string = 'sqlite:///SIMPLE.db'  # SQLite browser

    if RECREATE_DB and db_file_path.exists():
        os.remove(db_file)  # removes the current .db file if one already exists

    if not db_file_path.exists():
        create_database(db_connection_string)  # creates empty database based on the simple schema
        db = Database(db_connection_string)  # connects to the empty database
        db.load_database('data/')  # loads the data from the data files into the database
    else:
        db = Database(db_connection_string)  # if database already exists, connects to .db file

    return db

db = load_db()

# load table
ingest_table = Table.read('scripts/ingests/UltracoolSheet-Main.csv', data_start=1)

#Defining variables 
sources = ingest_table['name']
ra_lit = ingest_table['pmra_lit']
ra_lit_err = ingest_table['pmraerr_lit']
dec_lit = ingest_table['pmdec_lit']
dec_lit_err = ingest_table['pmdecerr_lit']
ref_pm_lit = ingest_table['ref_pm_lit']
#ra_UKIRT = ingest_table['pmra_UKIRT']
#ra_UKIRT_err = ingest_table['pmraerr_UKIRT']
#dec_UKIRT = ingest_table['pmdec_UKIRT']
#dec_UKIRT_err = ingest_table['pmdecerr_UKIRT']
#ref_pm_UKIRT = ingest_table['ref_plx_UKIRT']

#ingest_table_df = pd.DataFrame({'sources': sources, 'pm_ra' : ra_UKIRT, 'pm_ra_err' : ra_UKIRT_err, 'pm_dec' : dec_UKIRT, 'pm_dec_err' : dec_UKIRT_err, 'pm_ref' : ref_pm_UKIRT})



df = pd.read_csv('scripts/ingests/UltracoolSheet-Main.csv', usecols=['name' ,'pmra_lit', 'pmraerr_lit', 'pmdec_lit', 'pmdecerr_lit', 'ref_pm_lit']) .dropna()
df.reset_index(inplace=True, drop=True)

for i, ref in enumerate(df.ref_pm_lit):
    if ref == 'Dupu12':
        df.ref_pm_lit[i] = 'Dupu12a'
    if ref == 'VanL07':
        df.ref_pm_lit[i] = 'vanL07'
    if ref == 'Kend07a':
        df.ref_pm_lit[i] = 'Kend07'
    if ref == 'Lepi02b':
        df.ref_pm_lit[i] = 'Lepi02'
    if ref == "Mace13a":
        df.ref_pm_lit[i] = "Mace13"
    if ref == 'Kend03a':
        df.ref_pm_lit[i] = "Kend03"
    if ref == 'West08a':
        df.ref_pm_lit[i] = "West08"
    if ref == 'Reid05b':
        df.ref_pm_lit[i] = "Reid05"
    if ref == 'Burg08b':
        df.ref_pm_lit[i] = "Burg08c"
    if ref == 'Burg08c':
        df.ref_pm_lit[i] = "Burg08d"
    if ref == 'Burg08d':
        df.ref_pm_lit[i] = "Burg08b"
    if ref == 'Gagn15b':
        df.ref_pm_lit[i] = "Gagn15c"
    if ref == 'Gagn15c':
        df.ref_pm_lit[i] = "Gagn15b"
    if ref == 'Lodi07a':
        df.ref_pm_lit[i] = "Lodi07b"
    if ref == 'Lodi07b':
        df.ref_pm_lit[i] = "Lodi07a"
    if ref == 'Reid02c':
        df.ref_pm_lit[i] = "Reid02b"
    if ref == 'Reid06a':
        df.ref_pm_lit[i] = "Reid06b"
    if ref == 'Reid06b':
        df.ref_pm_lit[i] = "Reid06a"
    if ref == 'Scho04b':
        df.ref_pm_lit[i] = "Scho04a"
    if ref == 'Scho10a':
        df.ref_pm_lit[i] = "Scho10b"
    if ref == 'Tinn93b':
        df.ref_pm_lit[i] = "Tinn93c"
    if ref == 'Lieb79f':
        df.ref_pm_lit[i] = "Lieb79"
    if ref == 'Prob83c':
        df.ref_pm_lit[i] = "Prob83"
    if ref == 'Jame08a':
        df.ref_pm_lit[i] = 'Jame08'
    if ref == 'Lepi05a':
        df.ref_pm_lit[i] = 'Lepi05'
    if ref== 'Lodi05b':
        df.ref_pm_lit[i] = 'Lodi05'
    if ref=='Tinn95c':
        df.ref_pm_lit[i] = 'Tinn95'
names_data = [{'source': 'LP  649-93', 'other_name': '2MASS J02185792-0617499'}]
names_data.append({'source': 'WISE J014807.25-720258.7', 'other_name': 'WISEPC J014807.25-720258.7'}) 
names_data.append({'source': '2MASS J02484100-1651216', 'other_name': 'BR B0246-1703'})
names_data.append({'source': '2MASS J02192210-3925225', 'other_name': '2MASS J02192210-3925225A'})
names_data.append({'source': 'WISEP J031325.96+780744.2', 'other_name': 'WISEPA J031325.96+780744.2'})
names_data.append({'source': 'WISEA J033515.07+431044.7', 'other_name': 'WISE J033515.01+431045.1'})
names_data.append({'source': 'LP  413-53', 'other_name': '2MASS J03505737+1818069'})
names_data.append({'source': '2MASS J03510423+4810477', 'other_name': 'SDSS J035104.37+481046.8'})
names_data.append({'source': 'WISEP J041022.71+150248.5', 'other_name': 'WISEPA J041022.71+150248.5'})
names_data.append({'source': '2MASSI J0421072-630602', 'other_name': '2MASS J04210718-6306022'})
names_data.append({'source': '2MASS J04291842-3123568', 'other_name': '2MASSI J0429184-312356'})
names_data.append({'source': 'LP 775-31', 'other_name': 'LP 775-031'})
names_data.append({'source': 'LP 655- 48', 'other_name': 'NLTT 13728'})
names_data.append({'source': 'SSSPM J0511-4606', 'other_name': '2MASS J05110163-4606015'})
names_data.append({'source': 'DENIS J051737.7-334903', 'other_name': '2MASS J05173766-3349027'})
names_data.append({'source': '2MASS J05395200-0059019', 'other_name': 'SDSSp J053951.99-005902.0'})
names_data.append({'source': 'WISE J061135.13-041024.0', 'other_name': 'WISEPA J061135.13-041024.0'})
names_data.append({'source': '2MASS J06154934-0100415', 'other_name': 'DENIS-P J0615493-010041'})
names_data.append({'source': '2MASS J06420559+4101599', 'other_name': 'WISE J064205.58+410155.5'})
names_data.append({'source': 'LHS 1937', 'other_name': '2MASS J07410681+1738459'})
names_data.append({'source': '2MASS J08503593+1057156', 'other_name': '2MASSW J0850359+105716'})
names_data.append({'source': '2MASS J10393137+3256263', 'other_name': 'SDSS J103931.35+325625.5'})
names_data.append({'source': '2MASS J10481463-3956062', 'other_name': 'DENIS-P J104814.7-395606.1'})
names_data.append({'source': 'TWA 28', 'other_name': 'SSSPM J1102-3431'})
names_data.append({'source': 'SDSS J113454.91+002254.3', 'other_name': '2MASS J11345493+0022541'})
names_data.append({'source': 'WISEA J114156.67-332635.5', 'other_name': 'WISE J114156.71-332635.8'})
names_data.append({'source': '2MASS J11472421-2040204', 'other_name': 'WISEA J114724.10-204021.3'})
names_data.append({'source': 'SDSS J120358.19+001550.3', 'other_name': 'SDSSp J120358.19+001550.3'})
names_data.append({'source': '2MASS J12195156+3128497', 'other_name': 'SDSS J121951.45+312849.4'})
names_data.append({'source': '2MASS J12281523-1547342', 'other_name': 'DENIS-P J122813.8-154711'})
names_data.append({'source': '2MASS J12560183-1257276', 'other_name': 'VHS J125601.92-125723.9 b'})
names_data.append({'source': '2MASS J12560183-1257276', 'other_name': 'VHS J125601.92-125723.9 AB'})
names_data.append({'source': 'WISEP J131106.24+012252.4', 'other_name': 'WISEPC J131106.24+012252.4'})
names_data.append({'source': '2MASS J13114227+3629235', 'other_name': 'WISEP J131141.91+362925.2'})
names_data.append({'source': 'WISE J140518.39+553421.3', 'other_name': 'WISEPC J140518.40+553421.4'})
names_data.append({'source': '2MASS J14213145+1827407', 'other_name': '2MASSW J1421314+182740'})
names_data.append({'source': '2MASS J14250510+7102097', 'other_name': 'LSR J1425+7102'})
names_data.append({'source': '2MASSJ14442067-2019222', 'other_name': 'SSSPM J1444-2019'})
names_data.append({'source': 'LP  859-1', 'other_name': '2MASS J15041621-2355564'})
names_data.append({'source': 'WISE J154151.65-225024.9', 'other_name': 'WISEPA J154151.66-225025.2'})
names_data.append({'source': '2MASSI J1546271-332511', 'other_name': '2MASSI J1546291-332511'})
names_data.append({'source': '2MASS J15470557-1626303', 'other_name': 'PSO J236.7729-16.4422'})
names_data.append({'source': '2MASS J15470557-1626303', 'other_name': '2MASS J15470557-1626303B'})
names_data.append({'source': '2MASS J15474719-2423493', 'other_name': 'DENIS-P J154747.2-242349'})
names_data.append({'source': '[LDC2013] J160918.68-222923.9', 'other_name': 'USco J160918.69-222923.7'})
names_data.append({'source': '2MASS J16291840+0335371', 'other_name': 'PSO J247.3273+03.5932'})
names_data.append({'source': '2MASSI J1658037+702701', 'other_name': '2MASSW J1658037+702701'})
names_data.append({'source': 'SDSS J16585026+1820006', 'other_name': 'SDSS J165850.26+182000.6'})
names_data.append({'source': '2MASS J17125121-0507249', 'other_name': 'GJ 660.1B'})
names_data.append({'source': 'WISE J173835.53+273259.0', 'other_name': 'WISEPA J173835.53+273258.9'})
names_data.append({'source': 'WISEP J180435.40+311706.1', 'other_name': 'WISEPA J180435.40+311706.1'})
names_data.append({'source': 'WISEP J182831.08+265037.8', 'other_name': 'WISEPA J182831.08+265037.8'})
names_data.append({'source': '2MASS J18353790+3259545', 'other_name': '2MASSI J1835379+325954'})
names_data.append({'source': '2MASS J20362165+5100051', 'other_name': 'LSR J2036+5059'})
names_data.append({'source': 'WISEP J205628.90+145953.3', 'other_name': 'WISEPC J205628.90+145953.3'})
names_data.append({'source': '2MASS J21272613-4215183', 'other_name': '[HB88] M19'})
names_data.append({'source': '2MASS J21324036+1029494', 'other_name': 'SDSS J213240.36+102949.4'})
names_data.append({'source': '2MASS J21543454-1055308', 'other_name': 'SIMP J21543454-1055308'})
names_data.append({'source': '2MASS J23153135+0617146', 'other_name': 'PSO J348.8808+06.2873'})
names_data.append({'source': '2MASS J23310161-0406193', 'other_name': '2MASSI J2331016-040619'})
names_data.append({'source': 'SIPS J2346-3153', 'other_name': 'APMPM J2347-3154'})
names_data.append({'source': 'APMPM J2354-3316C', 'other_name': 'LHS 4039C'})
db.Names.insert().execute(names_data)

print(df)






#Ingesting lit pm into db
#ingest_proper_motions(db, sources, ra_lit, ra_lit_err, dec_lit, dec_lit_err, ref_pm_lit, save_db=False, verbose=False)

#Ingesting UKIRT pm into db
ingest_proper_motions(db, df.name, df.pmra_lit, df.pmraerr_lit, df.pmdec_lit, df.pmdecerr_lit, df.ref_pm_lit, save_db=False, verbose=False )

