import logging

from astropy.io import ascii
from scripts.ingests.utils import *
from scripts.ingests.ingest_utils import *
from astropy.utils.exceptions import AstropyWarning


warnings.simplefilter('ignore', category=AstropyWarning)

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.WARNING)


def convert_radec_to_decimal(source):
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

    return ra, dec


def add_publications(db):
    ingest_publication(db, publication='Meis20.74', bibcode='2020ApJ...889...74M')  # Meisner_2020a
    ingest_publication(db, publication='Meis20.123', bibcode='2020ApJ...899..123M')  # Meisner_2020b
    ingest_publication(db, bibcode='2020ApJ...895..145B')
    ingest_publication(db, bibcode='2021ApJS..253....7K')
    ingest_publication(db, bibcode='2019ApJ...881...17M')
    ingest_publication(db, bibcode='2021ApJ...918...11L')
    ingest_publication(db, bibcode='2020ApJ...889..176F')

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
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12a').values(
        publication='Lodi12.1495').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12b').values(
        publication='Lodi12.105').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12d').values(
        publication='Lodi12.53').execute()
    db.Publications.delete().where(db.Publications.c.publication == 'Lodi12').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Cush11').values(
        publication='Cush11.50').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06c').values(
        publication='Burg06.1095').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06e').values(
        publication='Burg06.585').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06b').values(
        publication='Burg06.1067').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06a').values(
        publication='Burg06.1007').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg06d').values(
        publication='Burg06.1485').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Liu_11a').values(
        publication='Liu_11.32').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Liu_11b').values(
        publication='Liu_11.108').execute()


def convert_ref_name(reference):
    # convert reference names
    first_ref = reference.split()[0]
    # print(first_ref)
    simple_ref = find_publication(db, first_ref)[1]
    # print(discovery_ref)
    return simple_ref


if RECREATE_DB:
    add_publications(db)

legg21_table = ascii.read('scripts/ingests/apjac0cfet10_mrt.txt', format='mrt')

# legg21_table.write('scripts/ingests/legg21.csv', overwrite=True, format='ascii.csv')

#  for all cases in the table, search and replace these strings
for row, source in enumerate(legg21_table):
    if source['DisRef'] == 'Meisner_2020a':
        legg21_table[row]['DisRef'] = 'Meis20.74'
    if source['r_SpType'] == 'Meisner_2020a':
        legg21_table[row]['r_SpType'] = 'Meis20.74'

    if source['DisRef'] == 'Meisner_2020b':
        legg21_table[row]['DisRef'] = 'Meis20.123'
    if source['r_SpType'] == 'Meisner_2020b':
        legg21_table[row]['r_SpType'] = 'Meis20.123'

    if source['DisRef'] == 'Mace_2013a':
        legg21_table[row]['DisRef'] = 'Mace13.6'
    if source['r_SpType'] == 'Mace_2013a':
        legg21_table[row]['r_SpType'] = 'Mace13.6'

    if source['DisRef'] == 'Mace_2013b':
        legg21_table[row]['DisRef'] = 'Mace13.36'
    if source['r_SpType'] == 'Mace_2013b':
        legg21_table[row]['r_SpType'] = 'Mace13.36'

    if source['DisRef'] == 'Pinfield_2014a':
        legg21_table[row]['DisRef'] = 'Pinf14.1009'
    if source['r_SpType'] == 'Pinfield_2014a':
        legg21_table[row]['r_SpType'] = 'Pinf14.1009'

    if source['DisRef'] == 'Pinfield_2014b':
        legg21_table[row]['DisRef'] = 'Pinf14.1931'

    if source['DisRef'] == 'Kirkpatrick_2012':
        legg21_table[row]['DisRef'] = 'Kirk12'
    if source['r_SpType'] == 'Kirkpatrick_2012':
        legg21_table[row]['r_SpType'] = 'Kirk12'

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
    if source['r_SpType'] == 'Burningham_2010b':
        legg21_table[row]['r_SpType'] = 'Burn10.1885'

    if source['DisRef'] == 'Burgasser_2002':
        legg21_table[row]['DisRef'] = 'Burg02.421'

    if source['r_SpType'] == 'Burgasser_2006':
        legg21_table[row]['r_SpType'] = 'Burg06.1067'

    if source['r_SpType'] == 'Burgasser_2010b':  # Error in Leggett 2021
        legg21_table[row]['r_SpType'] = 'Burg02.421'

    if source['DisRef'] == 'Warren_2007':
        legg21_table[row]['DisRef'] = 'Warr07.1400'
    if source['DisRef'] == 'Burgasser_2004':
        legg21_table[row]['DisRef'] = 'Burg04.2856'
    if source['DisRef'] == 'Lodieu_2009':
        legg21_table[row]['DisRef'] = 'Lodi09.258'

    if source['DisRef'] == 'Cushing_2011':
        legg21_table[row]['DisRef'] = 'Cush11.50'
    if source['r_SpType'] == 'Cushing_2011':
        legg21_table[row]['r_SpType'] = 'Cush11.50'

    if source['r_SpType'] == 'this_work':
        legg21_table[row]['r_SpType'] = 'Legg21'

    if source['r_SpType'] == 'Liu_2011':
        legg21_table[row]['r_SpType'] = 'Liu_11.108'

    if source['r_SpType'] == 'Lodieu_2012':
        legg21_table[row]['r_SpType'] = 'Lodi12.53'

    if source['r_SpType'] == 'Burningham_2010a':
        legg21_table[row]['r_SpType'] = 'Burn10.1885'
    # write out modified table

# print(legg21_table['DisRef'])

#  Check if all sources are in the database
# legg21_sources = legg21_table['Survey', 'RA', 'Decl.', 'DisRef']
source_strings = []
discovery_refs = []
ras = []
decs = []
other_references = []
sp_types = []
sp_type_refs = []


for row, source in enumerate(legg21_table):
    source_string = f"{source['Survey']} J{source['RA']}{source['Decl.']}"
    source_strings.append(source_string)

    # ra, dec = convert_radec_to_decimal(source)
    # ras.append(ra)
    # decs.append(dec)
    # print(row, source_string, ra, dec)

    #  Deal with discover ref and references
    # discovery_ref = convert_ref_name(source['DisRef'])
    # discovery_refs.append(discovery_ref)
    # try:
    #     legg21_second_ref = source['DisRef'].split()[1]
    #     second_ref = convert_ref_name(legg21_second_ref)
    #     other_references.append(second_ref)
    #     # print(f"other ref: {ref}")
    # except IndexError:
    #     other_references.append(None)

    # convert Legg21 SpType to string
    legg21_sptype_code_base = source['SpType'].split('.')[0]
    half_int = source['SpType'].split('.')[1][0]
    if half_int == '5':
        half_int_string = '.5'
    elif half_int == '0':
        half_int_string = ''
    else:
        msg = f"Unexpected half integer {half_int} for {source_string}"
        raise SimpleError(msg)

    try:
        suffix = source['SpType'].split()[1]
    except IndexError:
        suffix = None

    if float(legg21_sptype_code_base) <= 9:
        sp_type_string = f"T{legg21_sptype_code_base}{half_int_string}"
    elif float(legg21_sptype_code_base) >= 10:
        sp_type_string = f"Y{legg21_sptype_code_base[1]}{half_int_string}"
    else:
        msg = "Unexpected spectral type code"
        raise SimpleError
    if suffix:
        sp_type_string = f"{sp_type_string} {suffix}"

    sp_types.append(sp_type_string)

    sp_type_ref = convert_ref_name(source['r_SpType'])
    print(source_string, sp_type_ref)
    sp_type_refs.append(sp_type_ref)

# ingest_sources(db, source_strings, ras=ras, decs=decs,
#               references=discovery_refs, other_references=other_references)

# ingest spectral types
ingest_spectral_types(db, source_strings, sp_types, sp_type_refs)


# ingest NIR photometry

# ingest Spitzer photometry

# ingest WISE photometry

# ingest Other Names