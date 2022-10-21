from astropy.io import ascii
from scripts.ingests.utils import *
from scripts.ingests.ingest_utils import *
from astropy.utils.exceptions import AstropyWarning


warnings.simplefilter('ignore', category=AstropyWarning)


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)


def add_publications(db):
    ingest_publication(db, publication='Meis20.74', bibcode='2020ApJ...889...74M')  # Meisner_2020a
    ingest_publication(db, publication='Meis20.123', bibcode='2020ApJ...899..123M')  # Meisner_2020b
    ingest_publication(db, bibcode='2020ApJ...895..145B')
    ingest_publication(db, bibcode='2021ApJS..253....7K')
    ingest_publication(db, bibcode='2019ApJ...881...17M')

    db.Publications.update().where(db.Publications.c.publication == 'Mace13').values(publication='Mace13.6').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Mace13b').values(publication='Mace13.36').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Pinf14').values(publication='Pinf14.1931').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Pinf14a').values(publication='Pinf14.1009').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Scho10a').values(
        publication='Scho10.92').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Scho10b').values(
        publication='Scho10.8').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Tinn05a').values(
        publication='Tinn05.1171').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Tinn05b').values(
        publication='Tinn05.2326').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Delo08a').values(
        publication='Delo08.961').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Delo08b').values(
        publication='Delo08.469').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burn10').values(
        publication='Burn10.1952').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burn10b').values(
        publication='Burn10.1885').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg02a').values(
        publication='Burg02.421').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg02b').values(
        publication='Burg02.2744').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg02c').values(
        publication='Burg02.151').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Warr07').values(
        publication='Warr07.1400').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Warr07a').values(
        publication='Warr07.213').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04c').values(
        publication='Burg04.73').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04a').values(
        publication='Burg04.827').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04b').values(
        publication='Burg04.2856').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04d').values(
        publication='Burg04.191').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi09b').values(
        publication='Lodi09.1631').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi09d').values(
        publication='Lodi09.258').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Cush11').values(
        publication='Cush11.50').execute()


if RECREATE_DB:
    add_publications(db)

legg21_table = ascii.read('scripts/ingests/apjac0cfet10_mrt.txt', format='mrt')

# legg21_table.write('scripts/ingests/legg21.csv', overwrite=True, format='ascii.csv')

#  for all cases in the table, search and replace these strings
for row, source in enumerate(legg21_table):
    if source['DisRef'] == 'Meisner_2020a':
        legg21_table[row]['DisRef'] = 'Meis20.74'
    if source['DisRef'] == 'Meisner_2020b':
        legg21_table[row]['DisRef'] = 'Meis20.123'
    if source['DisRef'] == 'Mace_2013a':
        legg21_table[row]['DisRef'] = 'Mace13.6'
    if source['DisRef'] == 'Mace_2013b':
        legg21_table[row]['DisRef'] = 'Mace13.36'
    if source['DisRef'] == 'Pinfield_2014a':
        legg21_table[row]['DisRef'] = 'Pinf14.1009'
    if source['DisRef'] == 'Pinfield_2014b':
        legg21_table[row]['DisRef'] = 'Pinf14.1931'
    if source['DisRef'] == 'Kirkpatrick_2012':
        legg21_table[row]['DisRef'] = 'Kirk12'
    if source['DisRef'] == 'Scholz_2010a':
        legg21_table[row]['DisRef'] = 'Scho10.8'
    if source['DisRef'] == 'Scholz_2010b':
        legg21_table[row]['DisRef'] = 'Scho10.92'
    if source['DisRef'] == 'Tinney_2005':
        legg21_table[row]['DisRef'] = 'Tinn05.2326'
    if source['DisRef'] == 'Delorme_2008':
        legg21_table[row]['DisRef'] = 'Delo08.961'
    if source['DisRef'] == 'Burningham_2010a':
        legg21_table[row]['DisRef'] = 'Burn10.1952'
    if source['DisRef'] == 'Burningham_2010b':
        legg21_table[row]['DisRef'] = 'Burn10.1885'
    if source['DisRef'] == 'Burgasser_2002':
        legg21_table[row]['DisRef'] = 'Burg02.421'
    if source['DisRef'] == 'Warren_2007':
        legg21_table[row]['DisRef'] = 'Warr07.1400'
    if source['DisRef'] == 'Burgasser_2004':
        legg21_table[row]['DisRef'] = 'Burg04.2856'
    if source['DisRef'] == 'Lodieu_2009':
        legg21_table[row]['DisRef'] = 'Lodi09.258'
    if source['DisRef'] == 'Cushing_2011':
        legg21_table[row]['DisRef'] = 'Cush11.50'
    # write out modified table

# print(legg21_table['DisRef'])

#  Check if all sources are in the database
legg21_sources = legg21_table['Survey', 'RA', 'Decl.', 'DisRef']
source_strings = []
discovery_refs = []
ras = []
decs = []
other_references = []

for row, source in enumerate(legg21_sources):
    source_string = f"{source['Survey']} J{source['RA']}{source['Decl.']}"
    source_strings.append(source_string)

    # convert ra and dec to decimal string
    ra_seconds = source['RA'][4:]
    if float(ra_seconds) > 60.0:
        ra_seconds = f"{source['RA'][4:6]}.{source['RA'][6:]}"
    ra_sexg = f"{source['RA'][0:2]}h{source['RA'][2:4]}m{ra_seconds}s"
    dec_seconds = source['Decl.'][5:]
    if float(dec_seconds) > 60.0:
        dec_seconds = f"{source['Decl.'][5:7]}.{source['Decl.'][7:]}"
    dec_sexg = f"{source['Decl.'][0:3]}d{source['Decl.'][3:5]}m{dec_seconds}s"
    coord = SkyCoord(ra_sexg, dec_sexg)
    ra = coord.ra.degree
    dec = coord.dec.degree
    ras.append(ra)
    decs.append(dec)

    print(row, source_string, ra, dec)

    # convert reference names
    first_ref = source['DisRef'].split()[0]
    # print(first_ref)
    discovery_ref = find_publication(db, first_ref)
    # print(discovery_ref)
    ref = discovery_ref[1]
    # print(ref)
    discovery_refs.append(ref)
    try:
        second_ref = source['DisRef'].split()[1]
        other_ref = find_publication(db, second_ref)
        ref = other_ref[1]
        other_references.append(ref)
        # print(f"other ref: {ref}")
    except IndexError:
        other_references.append(None)


ingest_sources(db, source_strings, ras=ras, decs=decs,
               references=discovery_refs, other_references=other_references)