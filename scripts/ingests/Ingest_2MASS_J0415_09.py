"""

Data to be Ingested:
- Akari Spectrum
- FIRE Magellan Spectrum 
- KECK LRIS Spectrum Burg '03
- Keck LRIS Spectrum Kirk '08

Key Functions:
- Ingest_Spectra, Ingest Publication, Ingest Instrument

"""

from astrodb_utils import * # load_astrodb, ingest_instrument, ingest_publication, find_publication
from simple.utils.spectra import * # ingest_spectrum
from simple.schema import Spectra
from simple.schema import *
from simple.schema import REFERENCE_TABLES

#Load in Database
db = load_astrodb("SIMPLE.sqlite", recreatedb=True, reference_tables=REFERENCE_TABLES)

#Dictionary of new spectra
spectras = [

    {
        "file": "https://bdnyc.s3.us-east-1.amazonaws.com/AKARI_2MASS%2BJ04151954-0935066_2007-08-23.fits",
        "source": "2MASS J04151954-0935066",
        "regime": "nir",
        "telescope": "AKARI",
        "instrument": "IRC",
        "mode": "NIR",
        "obs_date": "2007-08-23",
        "reference": "Sora12",
        "bibcode": "2012ApJ...760..151S", 
        "comments":"AKARI Observations of Brown Dwarfs. III. CO, CO2, and CH4 Fundamental Bands and Physical Parameters",
        "doi": "10.1088/0004-637X/760/1/6",

    },
    {
        "file": "https://bdnyc.s3.us-east-1.amazonaws.com/FIRE/FIRE_0415-0935.fits",
        "source": "2MASS J04151954-0935066",
        "regime": "nir",
        "telescope": "Magellan I Baade",
        "instrument": "FIRE",
        "mode": "Echelle",
        "obs_date": "2010-09-20",
        "reference": "Simc13",
        "comments": "FIRE: A Facility Class Near-Infrared Echelle Spectrometer for the Magellan Telescopes", 
        "bibcode": "2013PASP..125..270S", 
        "doi": "10.1086/670241",
    },
    {
        "file": "https://bdnyc.s3.us-east-1.amazonaws.com/LRIS/KECK_LRIS_2MASSJ04151954-0935066_T8_LRIS_Burg03.fits",
        "source": "2MASS J04151954-0935066",
        "regime": "nir",
        "telescope": "KECK I",
        "instrument": "LRIS",
        "mode": "Missing",
        "obs_date": "2001-02-20",
        "reference": "Burg03",
        "comments": "The Spectra of T Dwarfs. II. Red Optical Data",
        "bibcode": "2003ApJ...594..510B", 
        "doi": "10.1086/376756",

    },
    {
        "file": "https://bdnyc.s3.us-east-1.amazonaws.com/LRIS/KECK_LRIS_2MASS_J04151954-0935066_2006-12-26.fits",
        "source": "2MASS J04151954-0935066",
        "regime": "nir",
        "telescope": "KECK I",
        "instrument": "LRIS",
        "mode": "Missing",
        "obs_date": "2006-12-26",
        "reference": "Kirk08",
        "comments": "A Sample of Very Young Field L Dwarfs and Implications for the Brown Dwarf 'Lithium Test' at Early Ages",
        "bibcode": "2008ApJ...689.1295K", 
        "doi": "10.1086/592768",
    }
]


#Ingest data
for spectrum in spectras:
    
    ingest_instrument(
        db,
        telescope=spectrum["telescope"],
        instrument=spectrum["instrument"],
        mode=spectrum["mode"],
    )

    status, details = find_publication(db, reference=spectrum["reference"])
    if status == False:
        ingest_publication(
            db,
            reference=spectrum["reference"],
            bibcode=spectrum['bibcode'],
            doi=spectrum['doi'],
            ignore_ads=True,
        )


    ingest_spectrum(
        db,
        source=spectrum["source"],
        spectrum=spectrum["file"],
        regime=spectrum["regime"],
        telescope=spectrum["telescope"],
        instrument=spectrum["instrument"],
        mode=spectrum["mode"],
        obs_date=spectrum["obs_date"],
        reference=spectrum["reference"],
        comments=spectrum['comments']

    )

# Save to Database
db.save_database(directory="data/")
