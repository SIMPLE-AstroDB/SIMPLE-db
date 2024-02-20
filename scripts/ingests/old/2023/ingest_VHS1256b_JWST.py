"""
Basic script for ingesting JWST spectrum for VHS1256b
"""

# pylint: disable=no-member

import logging
from datetime import datetime
from scripts.ingests.utils import logger, load_simpledb, ingest_publication

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Find identifier
source_data = db.search_object('VHS 1256')
source = source_data[0]['source']

# Confirm telescope/instrument exists
t = db.query(db.Telescopes).filter(db.Telescopes.c.telescope == 'JWST').table()
if len(t) == 0:
    # Ingest
    telescope_data = [{'telescope': 'JWST', 'description': 'James Webb Space Telescope'}]
    instruments_data = [
        {'instrument': 'NIRSpec', 'mode': 'IFU', 'telescope': 'JWST'},
        {'instrument': 'NIRSpec', 'mode': 'MOS', 'telescope': 'JWST'},
        {'instrument': 'NIRSpec', 'mode': 'BOTS', 'telescope': 'JWST'},
        {'instrument': 'MIRI', 'mode': 'IFU', 'telescope': 'JWST'},
        {'instrument': 'MIRI', 'mode': 'MRS', 'telescope': 'JWST'},
        {'instrument': 'MIRI', 'mode': 'LRS', 'telescope': 'JWST'},
        {'instrument': 'NIRCam', 'mode': 'TSO', 'telescope': 'JWST'},
        {'instrument': 'NIRCam', 'mode': 'WFSS', 'telescope': 'JWST'},
        {'instrument': 'NIRISS', 'mode': 'WFSS', 'telescope': 'JWST'},
        {'instrument': 'NIRISS', 'mode': 'SOSS', 'telescope': 'JWST'},
    ]
    with db.engine.connect() as conn:
        conn.execute(db.Telescopes.insert().values(telescope_data))
        conn.execute(db.Instruments.insert().values(instruments_data))
        conn.commit()

# Ingest publication
ingest_publication(db, doi='10.3847/2041-8213/acb04a')
ref_data = db.query(db.Publications).filter(db.Publications.c.doi == '10.3847/2041-8213/acb04a').table()
reference = ref_data[0]['reference']

# Prepare spectra data to ingest
spectra_data = [
    {'source': source,
     'spectrum': 'https://bdnyc.s3.amazonaws.com/JWST/NIRSpec/VHS1256b_NIR.fits',
     'original_spectrum': 'https://bdnyc.s3.amazonaws.com/JWST/VHS1256b_V2.txt',
     'telescope': 'JWST',
     'instrument': 'NIRSpec',
     'mode': 'IFU',
     'regime': 'nir',
     'observation_date': datetime.fromisoformat('2022-07-05'),
     'reference': reference
     },
    {'source': source,
     'spectrum': 'https://bdnyc.s3.amazonaws.com/JWST/MIRI/VHS1256b_MIRI.fits',
     'original_spectrum': 'https://bdnyc.s3.amazonaws.com/JWST/VHS1256b_V2.txt',
     'telescope': 'JWST',
     'instrument': 'MIRI',
     'mode': 'MRS',
     'regime': 'mir',
     'observation_date': datetime.fromisoformat('2022-07-05'),
     'reference': reference
     },
]

# Actually ingest spectra
with db.engine.connect() as conn:
    conn.execute(db.Spectra.insert().values(spectra_data))
    conn.commit()

# Check contents
_ = db.inventory(source, pretty_print=True)

# Save the files to disk
if SAVE_DB:
    db.save_database(directory='data/')
