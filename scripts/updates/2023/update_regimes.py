from astrodb_scripts import load_astrodb
from sqlalchemy import func
from simple.schema import *

# Establish connection to database
db = load_astrodb("SIMPLE.sqlite", recreatedb=True)


# Get list of units
t = (
    db.query(db.Spectra.c.regime, func.count(db.Spectra.c.regime).label("Counts"))
    .group_by(db.Spectra.c.regime)
    .all()
)
for row in t:
    print(row)


with db.engine.connect() as conn:
    conn.execute(
        db.Spectra.update()
        .where(db.Spectra.c.regime == "em.IR.NIR")
        .values(regime="nir")
    )
    conn.execute(
        db.Spectra.update()
        .where(db.Spectra.c.regime == "em.opt")
        .values(regime="optical")
    )
    conn.commit()


with db.engine.connect() as conn:
    conn.execute(
        db.Regimes.insert().values(
            regime="nir", description="Near-infrared 1-5 microns"
        )
    )
    conn.execute(
        db.Regimes.insert().values(
            regime="optical", description="Optical 3000-10000 Angstroms"
        )
    )
    conn.execute(
        db.Regimes.insert().values(
            regime="mir", description="Mid-infrared 5-30 microns"
        )
    )
    conn.commit()


# Save database
db.save_database("data/")
