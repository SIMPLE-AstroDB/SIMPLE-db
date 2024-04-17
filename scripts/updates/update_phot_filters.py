from astrodb_utils import load_astrodb
from simple.schema import *
from simple.schema import REFERENCE_TABLES
import pandas as pd

# Establish connection to database
db = load_astrodb("SIMPLE.sqlite", recreatedb=True, reference_tables=REFERENCE_TABLES)

# Run a query to find the bands with null UCDs
t = (
    db.query(
        db.PhotometryFilters.c.band,
        db.PhotometryFilters.c.ucd,
        db.PhotometryFilters.c.effective_wavelength,
    )
    .filter(db.PhotometryFilters.c.ucd.is_(None))
    .order_by(db.PhotometryFilters.c.effective_wavelength)
    .pandas()
)
df_new = t.drop(columns=["effective_wavelength"])
data_dicts = df_new.to_dict(orient="records")

data_dicts = [
    {"band": "GALEX.FUV", "ucd": "em.UV.100-200nm"},
    {"band": "GALEX.NUV", "ucd": "em.UV.200-300nm"},
    {"band": "SDSS.u", "ucd": "em.opt.U"},
    {"band": "Johnson.U", "ucd": "em.opt.U"},
    {"band": "Johnson.B", "ucd": "em.opt.B"},
    {"band": "SDSS.g", "ucd": "em.opt.B"},
    {"band": "GAIA2.Gbp", "ucd": "em.opt.B"},
    {"band": "Johnson.V", "ucd": "em.opt.V"},
    {"band": "SDSS.r", "ucd": "em.opt.R"},
    {"band": "Cousins.R", "ucd": "em.opt.R"},
    {"band": "SDSS.i", "ucd": "em.opt.I"},
    {"band": "ACAM.i", "ucd": "em.opt.I"},
    {"band": "DENIS.I", "ucd": "em.opt.I"},
    {"band": "UKIDSS.Z", "ucd": "em.opt.I"},
    {"band": "PS1.y", "ucd": "em.opt.I"},
    {"band": "VisAO.Ys", "ucd": "em.opt.I"},
    {"band": "NIRI.Y", "ucd": "em.opt.I"},
    {"band": "Wircam.Y", "ucd": "em.opt.I"},
    {"band": "GPI.Y", "ucd": "em.opt.I"},
    {"band": "NICMOS1.F110W", "ucd": "em.IR.J"},
    {"band": "Cousins.I", "ucd": "em.opt.I"},
    {"band": "SDSS.z", "ucd": "em.opt.I"},
    {"band": "ACAM.z", "ucd": "em.opt.I"},
    {"band": "UFTI.Y", "ucd": "em.opt.I"},
    {"band": "VISTA.Y", "ucd": "em.opt.I"},
    {"band": "DENIS.J", "ucd": "em.IR.J"},
    {"band": "NSFCam.J", "ucd": "em.IR.J"},
    {"band": "UFTI.J", "ucd": "em.IR.J"},
    {"band": "VISTA.J", "ucd": "em.IR.J"},
    {"band": "NSFCam.H", "ucd": "em.IR.H"},
    {"band": "UFTI.H", "ucd": "em.IR.H"},
    {"band": "VISTA.H", "ucd": "em.IR.H"},
    {"band": "NSFCam.Ks", "ucd": "em.IR.K"},
    {"band": "VISTA.Ks", "ucd": "em.IR.K"},
    {"band": "DENIS.Ks", "ucd": "em.IR.K"},
    {"band": "NSFCam.K", "ucd": "em.IR.K"},
    {"band": "UFTI.K", "ucd": "em.IR.K"},
    {"band": "WISE.W1", "ucd": "em.IR.3-4um"},
    {"band": "NSFCam.Lp", "ucd": "em.IR.3-4um"},
    {"band": "NACO.Lp", "ucd": "em.IR.3-4um"},
    {"band": "WISE.W2", "ucd": "em.IR.4-8um"},
    {"band": "NSFCam.Mp", "ucd": "em.IR.4-8um"},
    {"band": "NSFCam.M", "ucd": "em.IR.4-8um"},
    {"band": "WISE.W3", "ucd": "em.IR.8-15um"},
    {"band": "WISE.W4", "ucd": "em.IR.15-30um"},
]

with db.engine.connect() as conn:
    for data in data_dicts:
        conn.execute(
            db.PhotometryFilters.update()
            .where(
                db.PhotometryFilters.c.band == data["band"],
            )
            .values(ucd=data["ucd"])
        )
    conn.commit()

# db.save_database("data/")
db.save_reference_table("PhotometryFilters", "data/")
