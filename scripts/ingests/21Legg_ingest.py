import logging
from astropy.io import ascii
from scripts.ingests.utils import *
from scripts.ingests.ingest_utils import *
from astropy.utils.exceptions import AstropyWarning

# TODO: Figure out binaries
# TODO: figure out Burgasser10 duplicate

warnings.simplefilter('ignore', category=AstropyWarning)

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)


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
    ingest_publication(db, publication='Pinf14.priv',
                       description='P. Pinfield and M. Gromadzki, private communication 2014')

    db.Publications.update().where(db.Publications.c.publication == 'Burn10').values(
        publication='Burn10.1952').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burn10b').values(
        publication='Burn10.1885').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burn11').values(
        publication='Burn11.3590').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burn11b').values(
        publication='Burn11.90').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Burg00a').values(
        publication='Burg00.57').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg00b').values(
        publication='Burg00.473').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg00c').values(
        publication='Burg00.1100').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Burg02a').values(
        publication='Burg02.421').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg02b').values(
        publication='Burg02.2744').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg02c').values(
        publication='Burg02.151').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Burg03d').values(
        publication='Burg03.510').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg03b').values(
        publication='Burg03.512').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg03c').values(
        publication='Burg03.1186').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg03e').values(
        publication='Burg03.2487').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg03a').values(
        publication='Burg03.850').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Burg04c').values(
        publication='Burg04.73').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04a').values(
        publication='Burg04.827').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04b').values(
        publication='Burg04.2856').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Burg04d').values(
        publication='Burg04.191').execute()
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

    db.Publications.update().where(db.Publications.c.publication == 'Cush11').values(
        publication='Cush11.50').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Delo08a').values(
        publication='Delo08.961').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Delo08b').values(
        publication='Delo08.469').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Dupu15a').values(
        publication='Dupu15.102').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Dupu15b').values(
        publication='Dupu15.56').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Geli11').values(
        publication='Geli11.57').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Geli11b').values(
        publication='Geli11.871').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Lodi09b').values(
        publication='Lodi09.1631').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi09d').values(
        publication='Lodi09.258').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi07a').values(
        publication='Lodi07.1423').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi07b').values(
        publication='Lodi07.372').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12a').values(
        publication='Lodi12.1495').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12b').values(
        publication='Lodi12.105').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Lodi12d').values(
        publication='Lodi12.53').execute()
    db.Publications.delete().where(db.Publications.c.publication == 'Lodi12').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Loop07a').values(
        publication='Loop07.1162').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Loop07b').values(
        publication='Loop07.97').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Liu_11a').values(
        publication='Liu_11.32').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Liu_11b').values(
        publication='Liu_11.108').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Luhm12').values(
        publication='Luhm12.135').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm12d').values(
        publication='Luhm12.152').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14b').values(
        publication='Luhm14.18').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14d').values(
        publication='Luhm14.16').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14a').values(
        publication='Luhm14.4').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14c').values(
        publication='Luhm14.126').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Luhm14e').values(
        publication='Luhm14.6').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Mace13').values(
        publication='Mace13.6').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Mace13b').values(
        publication='Mace13.36').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Pinf14').values(
        publication='Pinf14.1931').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Pinf14a').values(
        publication='Pinf14.1009').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Scho10a').values(
        publication='Scho10.92').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Scho10b').values(
        publication='Scho10.8').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Tinn05a').values(
        publication='Tinn05.1171').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Tinn05b').values(
        publication='Tinn05.2326').execute()

    db.Publications.update().where(db.Publications.c.publication == 'Warr07').values(
        publication='Warr07.1400').execute()
    db.Publications.update().where(db.Publications.c.publication == 'Warr07a').values(
        publication='Warr07.213').execute()

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
    # if row == 319 or row == 215 or row == 216 or row == 210 or \
    #         row == 166 or row == 176 or row == 145 or row == 124 or \
    #         row ==320 or row ==326:
    #     # print(row, source['DisRef'])
    #     # print(row, source['r_SpType'])

    if source['DisRef'] == 'Burgasser_2000':
        legg21_table[row]['DisRef'] = 'Burg00.57'

    if source['DisRef'] == 'Burgasser_2002':
        legg21_table[row]['DisRef'] = 'Burg02.421'

    if source['DisRef'] == 'Burgasser_2003':
        legg21_table[row]['DisRef'] = 'Burg03.2487'
    if source['r_SpType'] == 'Burgasser_2003':
        legg21_table[row]['r_SpType'] = 'Burg03.2487'

    if source['r_SpType'] == 'Burgasser_2006':
        legg21_table[row]['r_SpType'] = 'Burg06.1067'

    if source['DisRef'] == 'Burgasser_2004':
        legg21_table[row]['DisRef'] = 'Burg04.2856'

    if 'Burningham_2010a' in source['DisRef']:
        b10_split = source['DisRef'].split()
        try:
            new = 'Burn10.1885 ' + b10_split[1]
        except IndexError:
            new = 'Burn10.1885'
        legg21_table[row]['DisRef'] = new
    if source['r_SpType'] == 'Burningham_2010a':
        legg21_table[row]['r_SpType'] = 'Burn10.1885'

    if 'Burningham_2010b' in source['DisRef']:
        b10b_split = source['DisRef'].split()
        try:
            if b10b_split[0] == 'Burningham_2010b':
                new = 'Burn10.1885 ' + b10b_split[1]
            elif b10b_split[1] == 'Burningham_2010b':
                new = b10b_split[0] + ' Burn10.1885 '
        except IndexError:
            new = 'Burn10.1885'
        legg21_table[row]['DisRef'] = new

    # TODO: Need to fix this
    if source['r_SpType'] == 'Burningham_2010b':
        legg21_table[row]['r_SpType'] = 'Burn10.1885'
    if source['r_SpType'] == 'Burgasser_2010b':  # Error in Leggett 2021
        legg21_table[row]['r_SpType'] = 'Burg02.421'

    if source['DisRef'] == 'Burningham_2011':
        legg21_table[row]['DisRef'] = 'Burn11.90'
    if source['r_SpType'] == 'Burningham_2011':
        legg21_table[row]['r_SpType'] = 'Burn11.90'

    if source['DisRef'] == 'Cushing_2011':
        legg21_table[row]['DisRef'] = 'Cush11.50'
    if source['r_SpType'] == 'Cushing_2011':
        legg21_table[row]['r_SpType'] = 'Cush11.50'

    if source['DisRef'] == 'Delorme_2008':
        legg21_table[row]['DisRef'] = 'Delo08.961'

    if source['r_SpType'] == 'Dupuy_2015':
        legg21_table[row]['r_SpType'] = 'Dupu15.102'

    if source['r_SpType'] == 'Gelino_2011':
        legg21_table[row]['r_SpType'] = 'Geli11.57'

    if 'Kirkpatrick_2012' in source['DisRef']:
        k12_split = source['DisRef'].split()
        try:
            new = 'Kirk12 ' + k12_split[1]
        except IndexError:
            new = 'Kirk12'
        legg21_table[row]['DisRef'] = new

    if source['r_SpType'] == 'Kirkpatrick_2012':
        legg21_table[row]['r_SpType'] = 'Kirk12'

    if source['DisRef'] == 'Kirkpatrick_2013':
        legg21_table[row]['DisRef'] = 'Kirk13'
    if source['r_SpType'] == 'Kirkpatrick_2013':
        legg21_table[row]['r_SpType'] = 'Kirk13'

    if source['DisRef'] == 'Lodieu_2007':
        legg21_table[row]['DisRef'] = 'Lodi07.1423'
    if source['r_SpType'] == 'Lodieu_2007':
        legg21_table[row]['r_SpType'] = 'Lodi07.1423'
    if source['DisRef'] == 'Lodieu_2009':
        legg21_table[row]['DisRef'] = 'Lodi09.258'
    if source['r_SpType'] == 'Lodieu_2009':
        legg21_table[row]['r_SpType'] = 'Lodi09.258'

    if source['DisRef'] == 'Lodieu_2012':
        legg21_table[row]['DisRef'] = 'Lodi12.53'
    if source['r_SpType'] == 'Lodieu_2012':
        legg21_table[row]['r_SpType'] = 'Lodi12.53'

    if source['DisRef'] == 'Looper_2007':
        legg21_table[row]['DisRef'] = 'Loop07.1162'
    if source['r_SpType'] == 'Looper_2007':
        legg21_table[row]['r_SpType'] = 'Loop07.1162'

    if source['r_SpType'] == 'this_work':
        legg21_table[row]['r_SpType'] = 'Legg21'

    if source['r_SpType'] == 'Liu_2011':
        legg21_table[row]['r_SpType'] = 'Liu_11.108'

    if source['r_SpType'] == 'Liu_2011':
        legg21_table[row]['r_SpType'] = 'Liu_11.108'

    if source['DisRef'] == 'Lucas_2010':
        legg21_table[row]['DisRef'] = 'Luca10'

    if source['DisRef'] == 'Luhman_2012':
        legg21_table[row]['DisRef'] = 'Luhm12.135'

    if source['DisRef'] == 'Luhman_2014':
        legg21_table[row]['DisRef'] = 'Luhm14.18'

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
    if source['r_SpType'] == 'Pinfield_2014b':
        legg21_table[row]['r_SpType'] = 'Pinf14.1931'

    if source['r_SpType'] == 'Pinfield_Gromadzki_2014':
        legg21_table[row]['r_SpType'] = 'Pinf14.priv'

    if 'Scholz_2010a' in source['DisRef']:
        s10_split = source['DisRef'].split()
        try:
            new = s10_split[0] + ' Scho10.8'
        except IndexError:
            new = 'Scho10.8'
        legg21_table[row]['DisRef'] = new

    if source['r_SpType'] == 'Scholz_2010a':
        legg21_table[row]['r_SpType'] = 'Scho10.8'

    if 'Scholz_2010b' in source['DisRef']:
        s10b_split = source['DisRef'].split()
        try:
            if s10b_split[0] == 'Scholz_2010b':
                new = 'Scho10.92 ' + s10b_split[1]
            elif s10b_split[1] == 'Scholz_2010b':
                new = s10b_split[0] + ' Scho10.92'
        except IndexError:
            new = 'Scho10.92'
        legg21_table[row]['DisRef'] = new

    if source['DisRef'] == 'Tinney_2005':
        legg21_table[row]['DisRef'] = 'Tinn05.2326'

    if source['DisRef'] == 'Warren_2007':
        legg21_table[row]['DisRef'] = 'Warr07.1400'

    # if row == 319 or row == 215 or row == 216 or row == 210 or \
    #         row == 166 or row == 176 or row == 145 or row == 124 or \
    #         row == 320 or row == 326:
    #     print(row, legg21_table[row]['DisRef'])
    #     print(row, legg21_table[row]['r_SpType'])

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

    ra, dec = convert_radec_to_decimal(source)
    ras.append(ra)
    decs.append(dec)
    print(row, source_string, ra, dec)

    #  Deal with discovery ref and references
    discovery_ref = convert_ref_name(source['DisRef'])
    discovery_refs.append(discovery_ref)
    try:
        legg21_second_ref = source['DisRef'].split()[1]
        second_ref = convert_ref_name(legg21_second_ref)
        other_references.append(second_ref)
        # print(f"other ref: {ref}")
    except IndexError:
        second_ref = ''
        other_references.append(None)

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
    print(source_string, discovery_ref, sp_type_ref, 'SECOND: ', second_ref)
    sp_type_refs.append(sp_type_ref)

ingest_sources(db, source_strings, ras=ras, decs=decs,
               references=discovery_refs, other_references=other_references)

ingest_spectral_types(db, source_strings, sp_types, sp_type_refs, regimes='nir')


# ingest NIR photometry

# ingest Spitzer photometry

# ingest WISE photometry

# ingest Other Names