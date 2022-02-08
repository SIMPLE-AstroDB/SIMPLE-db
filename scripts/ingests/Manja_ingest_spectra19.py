from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
# file = 'Manja19_spectra9.csv'
file = 'Manja19_spectra - Sheet1.csv'
data = Table.read('scripts/ingests/' + file)

# Ingest new instrumentation and telescope and mode

ingest_instrument(db, telescope='HST', instrument='WFC3', mode='G141 grism')

# Ingest new publications
ingest_publication(db, doi='10.1038/nature08245')
ingest_publication(db, doi='10.1093/pasj/62.6.L61')
ingest_publication(db, doi='10.1111/j.1365-2966.2009.14763.x')
ingest_publication(db, doi='10.1093/mnras/stw1969')
ingest_publication(db, doi='10.1051/0004-6361/201117081')
ingest_publication(db, doi='10.1093/mnras/stu1715')
ingest_publication(db, doi='10.1086/519793', publication='ODon07')
ingest_publication(db, bibcode='2011AAS...21710304R ')
ingest_publication(db, doi='10.1086/312458')
ingest_publication(db, doi='10.1086/586735')
ingest_publication(db, doi='10.1088/2041-8205/798/1/L13') # Yang15
ingest_publication(db, doi='10.3847/1538-3881/aaa5a6') # Bill18
# Update Buen15a to Buen15
db.Publications.update().where(db.Publications.c.publication == 'Buen15a').\
    values(publication='Buen15').execute()
ingest_publication(db, doi='10.3847/2041-8205/829/2/L32', publication='Lew_16') #Lew_16
ingest_publication(db, doi='10.1088/0004-637X/768/2/121') # Apai13
ingest_publication(db, doi='10.1088/2041-8205/760/2/L31') # Buen12
ingest_publication(db, doi='10.3847/0004-637X/826/1/8') # Yang16
ingest_publication(db, doi='10.3847/2041-8213/aa9ae9') # Shep17
ingest_publication(db, doi='10.1088/0004-637X/806/2/146') # Hayn15
ingest_publication(db, doi='10.1088/0004-637X/791/1/36', publication='Stev14.36') # Stev14.36
ingest_publication(db, doi='10.1126/science.1256758', publication='Stev14.758') #Stev14.758
ingest_publication(db, doi='10.1038/nature23266') # Evan17
ingest_publication(db, doi='10.3847/1538-3881/153/1/34') # Cart17
ingest_publication(db, doi='10.1088/0004-637X/785/2/148') # Ranj14
ingest_publication(db, doi='10.3847/1538-3881/aa899b') # Beat17
ingest_publication(db, doi='10.3847/0004-6256/152/6/203') # Line16
ingest_publication(db, doi='10.3847/1538-3881/aaabbd') #Zhou18

ingest_sources(db, 'NAME Luhman 16A', 'Luhm13', search_db=False)
ingest_sources(db, 'NAME Luhman 16B', 'Luhm13', search_db=False)

ingest_sources(db, data['Source'], data['discovery reference'])

# Fixing missing reference for Luhman 16
db.Sources.update().where(db.Sources.c.source == 'Luhman 16').values(reference='Luhm13').execute()

# Add the HST refs
ingest_publication(db, publication='Luca10.12217', bibcode='2010hst..prop12217L')
ingest_publication(db, publication='Apai11.12550', bibcode='2011hst..prop12550A')
ingest_publication(db, publication='Apai15.14241', bibcode=' 2015hst..prop14241A')
ingest_publication(db, publication='Apai12.13176', bibcode='2012hst..prop13176A')
ingest_publication(db, publication='Apai10.12341', bibcode='2010hst..prop12314A')
ingest_publication(db, bibcode='2016hst..prop14051R', publication='Radi16.14051')
ingest_publication(db, bibcode='2013hst..prop13299R', publication='Radi13.13299')
ingest_publication(db, bibcode='2013hst..prop13467B', publication='Bean13.13467')
ingest_publication(db, bibcode='2013hst..prop13308Z', publication='Zhao13.13308')
ingest_publication(db, bibcode='2014hst..prop13660Z', publication='Zhao14.13660')
ingest_publication(db, bibcode='2013hst..prop13280B', publication='Buen13.13280')
ingest_publication(db, bibcode='2015hst..prop14188B', publication='Bill15.14188')
ingest_publication(db, bibcode='2016hst..prop14767S', publication='Sing16.14767')
ingest_publication(db, bibcode='2010hst..prop12230S', publication='Swai10.12230')
ingest_publication(db, bibcode='2012hst..prop12970C', publication='Cush12.12970')
ingest_publication(db, bibcode='2011hst..prop12544C', publication='Cush11.12544')
ingest_publication(db, bibcode='2010hst..prop12330K', publication='Kirk10.12330')
ingest_publication(db, bibcode='2012hst..prop13178K', publication='Kirk12.13178')
ingest_publication(db, bibcode='2010hst..prop12181D', publication='Demi10.12181')
ingest_publication(db, bibcode='2011hst..prop12495D', publication='Demi11.12495')


# Add the spectra to the database
ingest_spectra(db, data['Source'], data['Spectrum'], 'em.IR.NIR', 'HST', 'WFC3', 'G141 grism',
               data['observation_date'],
               'Manj19', 'micron', 'erg/cm2/s/A', comments=data['spectrum comments'],
               other_references=data['Other spectrum refs'])

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
