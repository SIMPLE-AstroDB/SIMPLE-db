from sqlalchemy import func, and_
from astrodb_utils import load_astrodb
from simple.schema import *
from simple.schema import REFERENCE_TABLES

# Establish connection to database
db = load_astrodb("SIMPLE.sqlite", recreatedb=True, reference_tables=REFERENCE_TABLES)


t = (
    db.query(db.PhotometryFilters.c.band)
    .filter(db.PhotometryFilters.c.ucd is None)
    .all()
)
print

data_dicts = [
    {"band": "GALEX.FUV", "ucd": "em.UV"},
]
