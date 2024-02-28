from sqlalchemy import func, and_
from astrodb_scripts import load_astrodb
from simple.schema import *

# Establish connection to database
db = load_astrodb("SIMPLE.sqlite", recreatedb=True)


# Get list of regimes in the Spectra table
t = (
    db.query(db.Spectra.c.regime, func.count(db.Spectra.c.regime).label("Counts"))
    .group_by(db.Spectra.c.regime)
    .all()
)
for row in t:
    print(row)

# Fix the regimes in the Spectra table
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


# Get list of regimes in the Spectral Types table
t = (
    db.query(
        db.SpectralTypes.c.regime, func.count(db.SpectralTypes.c.regime).label("Counts")
    )
    .group_by(db.SpectralTypes.c.regime)
    .all()
)
for row in t:
    print(row)


# Find duplicate spectral types
with db.engine.connect() as conn:
    dupes = (
        db.query(
            db.SpectralTypes.c.source,
            db.SpectralTypes.c.reference,
            func.group_concat(db.SpectralTypes.c.regime).label("regimes"),
        )
        .group_by(
            db.SpectralTypes.c.source,
        )
        .having(func.count(db.SpectralTypes.c.source > 2))
        .order_by(db.SpectralTypes.c.regime)
        .table()
    )

dupes[3840:]

# Delete some duplicate spectral types
sources = [
    "WISE J154151.65-225024.9",
    "WISE J163940.83-684738.6",
    "WISEA J120604.25+840110.5",
    "WISE J140518.39+553421.3",
    "WISEA J082507.37+280548.2",
    "WISE J235402.77+024015.0",
]
with db.engine.connect() as conn:
    for source in sources:
        conn.execute(
            db.SpectralTypes.delete().where(
                and_(
                    db.SpectralTypes.c.source == source,
                    db.SpectralTypes.c.regime == "infrared",
                    db.SpectralTypes.c.reference == "Schn15",
                ),
            )
        )
    conn.commit()

# Delete some WISE source swith duplicate spectral types
sources = [
    "WISE J030449.03-270508.3",
    "WISE J033605.05-014350.4",
    "WISE J035000.32-565830.2",
    "WISE J035934.06-540154.6",
    "WISE J064723.23-623235.5",
    "WISE J071322.55-291751.9",
    "WISE J073444.02-715744.0",
    "WISE J173835.53+273259.0",
    "WISE J220905.73+271143.9",
    "WISE J222055.31-362817.4",
    "WISEA J114156.67-332635.5",
    "WISEP J041022.71+150248.5",
]
with db.engine.connect() as conn:
    for source in sources:
        conn.execute(
            db.SpectralTypes.delete().where(
                and_(
                    db.SpectralTypes.c.source == source,
                    db.SpectralTypes.c.regime == "infrared",
                ),
            )
        )
    conn.commit()


# "WISEP J182831.08+265037.8", WISEA J085510.74-071442.5
with db.engine.connect() as conn:
    # conn.execute(
    #     db.SpectralTypes.delete().where(
    #         and_(
    #             db.SpectralTypes.c.source == "WISEP J182831.08+265037.8",
    #             db.SpectralTypes.c.regime == "nir",
    #         ),
    #     )
    # )
    # conn.execute(
    #     db.SpectralTypes.delete().where(
    #         and_(
    #             db.SpectralTypes.c.source == "WISEA J085510.74-071442.5",
    #             db.SpectralTypes.c.regime == "nir",
    #         ),
    #     )
    # )
    # TODO: Figure out correct spectral type T6.5 or T7?
    # for 2MASSI J1553022+153236 from Burg06
    conn.execute(
        db.SpectralTypes.delete().where(
            and_(
                db.SpectralTypes.c.source == "2MASSI J1553022+153236",
                db.SpectralTypes.c.regime == "nir_UCD",
            ),
        )
    )

    conn.commit()


# Find sources with regime = nir_UCD and change one-by-one
# Starting with 1976 sources
# down to 1335
with db.engine.connect() as conn:
    nir_ucds = (
        db.query(db.SpectralTypes.c.source)
        .where(db.SpectralTypes.c.regime == "nir_UCD")
        .table()
    )
    for source in nir_ucds["source"]:
        print(source)
        conn.execute(
            db.SpectralTypes.update()
            .where(
                and_(
                    db.SpectralTypes.c.source == source,
                    db.SpectralTypes.c.regime == "nir_UCD",
                )
            )
            .values(regime="nir")
        )
        conn.commit()


# Fix the regimes in the Spectral Types table
with db.engine.connect() as conn:
    conn.execute(
        db.SpectralTypes.update()
        .where(
            db.SpectralTypes.c.regime == "nir_UCD",
        )
        .values(regime="nir")
    )
    conn.commit()

with db.engine.connect() as conn:
    conn.execute(
        db.SpectralTypes.update()
        .where(
            db.SpectralTypes.c.regime == "unknown",
        )
        .values(regime=None)
    )
    conn.commit()


# Populate the regimes table
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

with db.engine.connect() as conn:
    conn.execute(
        db.Regimes.insert().values(
            regime="unknown", description="Used in Spectral Types table. Delete in #309"
        )
    )
    conn.commit()

# Get list of regimes in the Spectral Typtes table

# Save database
db.save_database("data/")
