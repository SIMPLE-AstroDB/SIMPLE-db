from sqlalchemy import and_
import pytest


@pytest.mark.parametrize(
    "band, value",
    [
        ("GAIA2.G", 1267),
        ("GAIA2.Grp", 1107),
        ("GAIA3.G", 1256),
        ("GAIA3.Grp", 1261),
        ("WISE.W1", 461),
        ("WISE.W2", 461),
        ("WISE.W3", 457),
        ("WISE.W4", 450),
        ("2MASS.J", 1802),
        ("2MASS.H", 1791),
        ("2MASS.Ks", 1762),
        ("GPI.Y", 1),
        ("NIRI.Y", 21),
        ("UFTI.Y", 13),
        ("Wircam.Y", 29),
        ("WFCAM.Y", 854),
        ("VisAO.Ys", 1),
        ("VISTA.Y", 59),
        ("JWST/MIRI.F1000W", 1),
        ("JWST/MIRI.F1280W", 1),
        ("JWST/MIRI.F1800W", 1),
    ],
)
def test_photometry_bands(db, band, value):
    # To refresh these counts:
    # from sqlalchemy import func
    # db.query(db.Photometry.c.band, func.count(db.Photometry.c.band).label('count')).\
    #     group_by(db.Photometry.c.band).\
    #     astropy()

    t = db.query(db.Photometry).filter(db.Photometry.c.band == band).astropy()
    assert len(t) == value, f"found {len(t)} photometry measurements for {band}"


def test_photometrymko_y(db):
    # Test for Y photometry entries added for references
    bands_list = [
        "Wircam.Y",
        "WFCAM.Y",
        "NIRI.Y",
        "VISTA.Y",
        "GPI.Y",
        "VisAO.Ys",
        "UFTI.Y",
    ]
    ref_list = [
        "Albe11",
        "Burn08",
        "Burn09",
        "Burn10.1885",
        "Burn13",
        "Burn14",
        "Card15",
        "Deac11.6319",
        "Deac12.100",
        "Deac14.119",
        "Deac17.1126",
        "Delo08.961",
        "Delo12",
        "Dupu12.19",
        "Dupu15.102",
        "Dupu19 ",
        "Edge16",
        "Garc17.162",
        "Gauz12 ",
        "Kell16",
        "Lawr07",
        "Lawr12",
        "Legg13",
        "Legg15",
        "Legg16",
        "Liu_12",
        "Liu_13.20",
        "Liu_16",
        "Lodi07.372",
        "Lodi12.53",
        "Lodi13.2474",
        "Luca10",
        "Male14",
        "McMa13",
        "Minn17",
        "Naud14",
        "Pena11",
        "Pena12",
        "Pinf08",
        "Smit18",
        "Warr07.1400",
    ]

    t = (
        db.query(db.Photometry)
        .filter(
            and_(
                db.Photometry.c.band.in_(bands_list),
                db.Photometry.c.reference.in_(ref_list),
            )
        )
        .astropy()
    )
    assert len(t) == 969, f"found {len(t)} Y photometry entries"


def test_photometry_filters_ucds(db):
    # UCDS based on UCD1+ controlled vocabulary version 1.5
    # https://www.ivoa.net/documents/UCD1+/20230125/index.html
    phot_ucds = [
        "em.IR",
        "em.IR.J",
        "em.IR.H",
        "em.IR.K",
        "em.IR.3-4um",
        "em.IR.4-8um",
        "em.IR.8-15um",
        "em.IR.15-30um",
        "em.IR.30-60um",
        "em.IR.60-100um",
        "em.IR.NIR",
        "em.IR.MIR",
        "em.IR.FIR",
        "em.UV",
        "em.UV.10-50nm",
        "em.UV.50-100nm",
        "em.UV.100-200nm",
        "em.UV.200-300nm",
        "em.X-ray",
        "em.X-ray.soft",
        "em.X-ray.medium",
        "em.X-ray.hard",
        "em.gamma",
        "em.gamma.soft",
        "em.gamma.hard",
        "em.line.HI",
        "em.line.Lyalpha",
        "em.line.Halpha",
        "em.line.Hbeta",
        "em.line.Hgamma",
        "em.line.Hdelta",
        "em.line.Brgamma",
        "em.line.OIII",
        "em.line.CO",
        "em.mm",
        "em.mm.30-50GHz",
        "em.mm.50-100GHz",
        "em.mm.100-200GHz",
        "em.mm.200-400GHz",
        "em.mm.400-750GHz",
        "em.mm.750-1500GHz",
        "em.mm.1500-3000GHz",
        "em.opt",
        "em.opt.U",
        "em.opt.B",
        "em.opt.V",
        "em.opt.R",
        "em.opt.I",
        "em.radio",
        "em.radio.20MHz",
        "em.radio.20-100MHz",
        "em.radio.100-200MHz",
        "em.radio.200-400MHz",
        "em.radio.400-750MHz",
        "em.radio.750-1500MHz",
        "em.radio.1500-3000MHz",
        "em.radio.3-6GHz",
        "em.radio.6-12GHz",
        "em.radio.12-30GHz",
    ]

    t = db.query(db.PhotometryFilters.c.ucd).astropy()
    for ucd in t:
        assert ucd[0] in phot_ucds
