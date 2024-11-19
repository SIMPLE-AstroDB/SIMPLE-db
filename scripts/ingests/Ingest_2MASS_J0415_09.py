"""

Data to be Ingested:
- Akari Spectrum
- FIRE Magellan Spectrum 
- KECK LRIS Spectrum Burg '03
- Keck LRIS Spectrum Kirk '08

Key Functions:
- Ingest_Spectra, Ingest Publication, Ingest Instrument

"""

from astrodb_utils import * 
from simple.utils.spectra import * 
from simple.schema import Spectra
from simple.schema import *
from simple.schema import REFERENCE_TABLES

#Load in Database
db = load_astrodb("SIMPLE.sqlite", recreatedb=True, reference_tables=REFERENCE_TABLES)

Ingest data
ingest_publication(
    db, 
    bibcode = "2012ApJ...760..151S"
)

ingest_publication(
    db, 
    bibcode = "2013PASP..125..270S"
)

ingest_publication(
    db, 
    bibcode="2007PASJ...59S.369M"
)

ingest_publication(
    db,
    bibcode = "2007PASJ...59S.401O"
)

ingest_spectrum(
    db,
    source="2MASS J04151954-0935066",
    spectrum="https://bdnyc.s3.us-east-1.amazonaws.com/AKARI_2MASS%2BJ04151954-0935066_2007-08-23.fits",
    regime="nir",
    telescope="AKARI",
    instrument="IRC",
    mode="IRC04",
    obs_date="2007-08-23",
    reference="Sora12"

)

ingest_spectrum(
    db,
    source="2MASS J04151954-0935066",
    spectrum="https://bdnyc.s3.us-east-1.amazonaws.com/FIRE/FIRE_0415-0935.fits",
    regime="nir",
    telescope="Magellan I Baade",
    instrument="FIRE",
    mode="Echelle",
    obs_date="2010-09-20",
    reference="Simc13"

)

ingest_spectrum(
    db,
    source="2MASS J04151954-0935066",
    spectrum="https://bdnyc.s3.us-east-1.amazonaws.com/LRIS/KECK_LRIS_2MASS_J04151954-0935066_2006-12-26.fits",
    regime="optical",
    instrument="LRIS",
    telescope="Keck I",
    mode="Missing",
    obs_date="2006-12-26",
    reference="Kirk08"
)

ingest_spectrum(
    db,
    source="2MASS J04151954-0935066",
    spectrum="https://bdnyc.s3.us-east-1.amazonaws.com/LRIS/KECK_LRIS_2MASSJ04151954-0935066_T8_LRIS_Burg03.fits",
    regime="optical",
    instrument="LRIS",
    telescope="Keck I",
    mode="Missing",
    obs_date="2001-02-20",
    reference="Burg03.510"
)


# Save to Database
db.save_database(directory="data/")
