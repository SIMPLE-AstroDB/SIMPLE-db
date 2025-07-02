# Tests to verify database contents
# db is defined in conftest.py
import pytest
from sqlalchemy import except_, select, and_


# Utility functions
# -----------------------------------------------------------------------------------------
def reference_verifier(t, name, bibcode, doi):
    # Utility function to verify reference values in a table
    ind = t["reference"] == name
    assert t[ind]["bibcode"][0] == bibcode, f"{name} did not match bibcode"
    assert t[ind]["doi"][0] == doi, f"{name} did not match doi"


def test_sources(db):
    # Test to verify existing counts of sources and names
    n_sources = db.query(db.Sources).count()
    assert n_sources == 3640, f"found {n_sources} sources"

    n_names = db.query(db.Names).count()
    assert n_names == 9255, f"found {n_names} names"


@pytest.mark.parametrize(
    ("ref", "n_sources"),
    [
        ("Schm10.1808", 208),
        ("West08", 194),
        ("Reid08.1290", 206),
        ("Cruz03", 165),
        ("Maro15", 113),
        ("Best15", 101),
        ("Kirk11", 100),
        ("Mace13.6", 93),
        ("Burn13", 69),
        ("Gagn15.33", 68),
        ("Chiu06", 62),
        ("DayJ13", 61),
        ("Kirk10", 59),
        ("Cruz07", 91),
        ("Roth", 83),
        ("Deac14.119", 54),
        ("Hawl02", 51),
        ("Card15", 45),
        ("Burn10.1885", 43),
        ("Albe11", 37),
    ],
)
def test_discovery_references(db, ref, n_sources):
    """
    Values found with this SQL query:
        SELECT reference, count(*)
        FROM Sources
        GROUP BY reference
        ORDER By 2 DESC

        # Counting the top 20 references in the Sources Table
    # spec_ref_count = (
    #     db.query(Sources.reference, func.count(Sources.reference))
    #     .group_by(Sources.reference)
    #     .order_by(func.count(Sources.reference).desc())
    #     .limit(20)
    #     .all()
    # )

    """

    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == n_sources, f"found {len(t)} discovery reference entries for {ref}"


def test_missions(db):
    # If 2MASS designation in Names, 2MASS photometry should exist
    stm = except_(
        select(db.Names.c.source).where(db.Names.c.other_name.like("2MASS J%")),
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("2MASS%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 382
    ), f"found {len(s)} sources with 2MASS designation that have no 2MASS photometry"

    # If 2MASS photometry, 2MASS designation should be in Names
    stm = except_(
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("2MASS%")),
        select(db.Names.c.source).where(db.Names.c.other_name.like("2MASS J%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 2
    ), f"found {len(s)} sources with 2MASS photometry that have no 2MASS designation "

    # If Gaia designation in Names, Gaia photometry and astrometry should exist
    stm = except_(
        select(db.Names.c.source).where(db.Names.c.other_name.like("Gaia%")),
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("GAIA%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 10
    ), f"found {len(s)} sources with Gaia designation that have no GAIA photometry"

    # If Gaia photometry, Gaia designation should be in Names
    stm = except_(
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("GAIA%")),
        select(db.Names.c.source).where(db.Names.c.other_name.like("Gaia%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 0
    ), f"found {len(s)} sources with Gaia photometry and no Gaia designation in Names"

    # If Wise designation in Names, Wise phot should exist
    stm = except_(
        select(db.Names.c.source).where(db.Names.c.other_name.like("WISE%")),
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("WISE%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 496
    ), f"found {len(s)} sources with WISE designation that have no WISE photometry"

    # If Wise photometry, Wise designation should be in Names
    stm = except_(
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("WISE%")),
        select(db.Names.c.source).where(db.Names.c.other_name.like("WISE%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 389
    ), f"found {len(s)} sources with WISE photometry and no Wise designation in Names"

    # If Gaia EDR3 pm, Gaia EDR3 designation should be in Names
    stm = except_(
        select(db.ProperMotions.c.source).where(
            db.ProperMotions.c.reference.like("GaiaEDR3%")
        ),
        select(db.Names.c.source).where(db.Names.c.other_name.like("Gaia EDR3%")),
    )
    s = db.session.scalars(stm).all()
    msg = (
        f"found {len(s)} sources with Gaia EDR3 proper motion "
        "and no Gaia EDR3 designation in Names"
    )
    assert len(s) == 0, msg

    # If Gaia EDR3 parallax, Gaia EDR3 designation should be in Names
    stm = except_(
        select(db.Parallaxes.c.source).where(
            db.Parallaxes.c.reference.like("GaiaEDR3%")
        ),
        select(db.Names.c.source).where(db.Names.c.other_name.like("Gaia EDR3%")),
    )
    s = db.session.scalars(stm).all()
    msg = (
        f"found {len(s)} sources with Gaia EDR3 parallax "
        f"and no Gaia EDR3 designation in Names "
    )
    assert len(s) == 1, msg


# Test to verify existing counts of spectral types grouped by regime
@pytest.mark.parametrize(
    ("regime", "n_spectra"),
    [("optical", 1494), ("nir", 2644), ("mir", 0), ("unknown", 11)],
)
def test_spectral_types_regimes(db, regime, n_spectra):
    t = db.query(db.SpectralTypes).filter(db.SpectralTypes.c.regime == regime).astropy()
    assert len(t) == n_spectra, f"found {len(t)} spectral types in the {regime} regime"
    print(f"found {len(t)} spectral types in the {regime} regime")

# Test numbers of MLTY dwarfs
@pytest.mark.parametrize(
    ("string", "code_min", "code_max", "n_spectra"),
    [("M", 60, 70, 967), ("L", 70, 80, 2084), ("T", 80, 90, 1039), ("Y", 90, 100, 59)],
)  # Total number of MLTY dwarfs = 3863
def test_spectral_types_classes(db, string, code_min, code_max, n_spectra):
    result = (
        db.query(db.SpectralTypes)
        .filter(
            and_(
                db.SpectralTypes.c.spectral_type_code >= code_min,
                db.SpectralTypes.c.spectral_type_code < code_max,
            )
        )
        .astropy()
    )
    assert len(result) == n_spectra, f"found {len(result)} {string} spectral types"
    print(f"found {len(result)} {string} spectral types")


def test_spectral_types(db):
    n_spectral_types = db.query(db.SpectralTypes).count()
    assert n_spectral_types == 4149, f"found {n_spectral_types} spectral types"
    print(f"found {n_spectral_types} total spectral types")

    n_photometric_spectral_types = (
        db.query(db.SpectralTypes).filter(db.SpectralTypes.c.photometric == 1).count()
    )

    assert (
        n_photometric_spectral_types == 54
    ), f"found {n_photometric_spectral_types} photometric spectral types"
    print(f"found {n_photometric_spectral_types} photometric spectral types")

    n_adopted_spectral_types = (
        db.query(db.SpectralTypes).filter(db.SpectralTypes.c.adopted == 1).count()
    )
    assert (
        n_adopted_spectral_types == 288
    ), f"found {n_adopted_spectral_types} adopted spectral types"
    print(f"found {n_adopted_spectral_types} adopted spectral types")


@pytest.mark.parametrize(
    ("param", "n_counts"),
    [
        ("T eff", 176),
        ("log g", 176),
        ("mass", 176),
        ("radius", 175),
        ("metallicity", 2),
    ],
)
def test_modeledparameters_params(db, param, n_counts):
    # Test to verify existing counts of modeled parameters
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.parameter == param)
        .astropy()
    )
    assert (
        len(t) == n_counts
    ), f"found {len(t)} modeled parameters with {param} parameter"


@pytest.mark.parametrize(
    ("ref", "n_counts"),
    [
        ("Fili15", 696),
        ("Lodi22", 5),
    ],
)
def test_modeledparameters_refs(db, ref, n_counts):
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.reference == ref)
        .astropy()
    )
    assert len(t) == n_counts, f"found {len(t)} modeled parameters with {ref} reference"


def test_companion_relations(db):
    t = db.query(db.CompanionRelationships).astropy()
    assert len(t) == 194, f"found {len(t)} companion relationships"

    ref = "Roth24"
    t = (
        db.query(db.CompanionRelationships)
        .filter(db.CompanionRelationships.c.reference == ref)
        .astropy()
    )
    assert len(t) == 89, f"found {len(t)} companion relationships with {ref} reference"


@pytest.mark.parametrize(
    ("ref", "n_counts"),
    [
        ("Roth24", 18),
        ("GaiaDR3", 50),
    ],
)
def test_companionparameters_ref(db, ref, n_counts):
    t = (
        db.query(db.CompanionParameters)
        .filter(db.CompanionParameters.c.reference == ref)
        .astropy()
    )
    assert len(t) == n_counts, f"found {len(t)} companion parameters with {ref} reference"

@pytest.mark.parametrize(
    ("param", "n_counts"),
    [
        ("age", 24),
        ("metallicity", 54),
    ],
)
def test_companionparameters_params(db, param, n_counts):
    # Test to verify existing counts of modeled parameters
    t = (
        db.query(db.CompanionParameters)
        .filter(db.CompanionParameters.c.parameter == param)
        .astropy()
    )
    assert len(t) == n_counts, f"found {len(t)} modeled parameters with {param} parameter"


# Individual ingest tests
# -----------------------------------------------------------------------------------------
def test_Manj19_data(db):
    """
    Tests for data ingested from Manjavacas+2019

    Data file(s):
        ATLAS_table.vot
        Manj19_spectra - Sheet1.csv

    Ingest script(s):
        ingest_ATLAS_sources.py
        ingest_ATLAS_spectral_types.py
        Manja_ingest_spectra19.py
    """

    pub = "Manj19"

    # Check DOI and Bibcode values are correctly set for new publications added
    manj19_pub = (
        db.query(db.Publications).filter(db.Publications.c.reference == pub).astropy()
    )
    reference_verifier(
        manj19_pub, "Manj19", "2019AJ....157..101M", "10.3847/1538-3881/aaf88f"
    )

    # Test total spectral types added
    n_Manj19_types = (
        db.query(db.SpectralTypes).filter(db.SpectralTypes.c.reference == pub).count()
    )
    assert n_Manj19_types == 40, f"found {n_Manj19_types} sources for {pub}"

    # Test number of L types added
    n_Manj19_Ltypes = (
        db.query(db.SpectralTypes)
        .filter(
            and_(
                db.SpectralTypes.c.spectral_type_code >= 70,
                db.SpectralTypes.c.spectral_type_code < 80,
                db.SpectralTypes.c.reference == pub,
            )
        )
        .count()
    )
    assert n_Manj19_Ltypes == 19, f"found {n_Manj19_Ltypes} L type dwarfs for {pub}"

    # Test number of T types added
    n_Manj19_Ttypes = (
        db.query(db.SpectralTypes)
        .filter(
            and_(
                db.SpectralTypes.c.spectral_type_code >= 80,
                db.SpectralTypes.c.spectral_type_code < 90,
                db.SpectralTypes.c.reference == pub,
            )
        )
        .count()
    )
    assert n_Manj19_Ttypes == 21, f"found {n_Manj19_Ttypes} T type dwarfs for {pub}"

    # Test spectra added
    n_Manj19_spectra = (
        db.query(db.Spectra).filter(db.Spectra.c.reference == pub).astropy()
    )
    assert (
        len(n_Manj19_spectra) == 77
    ), f"found {len(n_Manj19_spectra)} spectra from {pub}"


def test_Kirk19_ingest(db):
    """
    Tests for Y-dwarf data ingested from Kirkpartick+2019

    Data file(s):
        Y-dwarf_table.csv

    Ingest script(s):
        Y_dwarf_source_ingest.py
        Y-dwarf_SpT_ingest.py
        Y-dwarf_astrometry-ingest.py
        Y_dwarf_pm_ingest.py

    """

    # Reference tests
    # -----------------------------------------------------------------------------------------

    # Test refereces added
    ref_list = [
        "Tinn18",
        "Pinf14.1931",
        "Mace13.6",
        "Kirk12",
        "Cush11.50",
        "Kirk13",
        "Schn15",
        "Luhm14.18",
        "Tinn14",
        "Tinn12",
        "Cush14",
        "Kirk19",
    ]
    t = (
        db.query(db.Publications)
        .filter(db.Publications.c.reference.in_(ref_list))
        .astropy()
    )

    if len(ref_list) != len(t):
        missing_ref = list(set(ref_list) - set(t["name"]))
        assert len(ref_list) == len(t), f"Missing references: {missing_ref}"

    # Check DOI and Bibcode values are correctly set for new references added
    reference_verifier(t, "Kirk19", "2019ApJS..240...19K", "10.3847/1538-4365/aaf6af")
    reference_verifier(t, "Pinf14.1931", "2014MNRAS.444.1931P", "10.1093/mnras/stu1540")
    reference_verifier(t, "Tinn18", "2018ApJS..236...28T", "10.3847/1538-4365/aabad3")

    # Data tests
    # -----------------------------------------------------------------------------------------

    # Test sources added
    ref = "Pinf14.1931"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 1, f"found {len(t)} sources for {ref}"

    # Test proper motions added
    ref = "Kirk19"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 182, f"found {len(t)} proper motion entries for {ref}"


def test_Best2020_ingest(db):
    # Test for Best20.257 proper motions added
    ref = "Best20.257"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 348, f"found {len(t)} proper motion entries for {ref}"

    # Test for Best20.257 parallaxes added
    ref = "Best20.257"
    t = db.query(db.Parallaxes).filter(db.Parallaxes.c.reference == ref).astropy()
    assert len(t) == 348, f"found {len(t)} parallax entries for {ref}"
    # Test for number of Best20.257 parallaxes that are adopted
    t = (
        db.query(db.Parallaxes)
        .filter(and_(db.Parallaxes.c.reference == ref, db.Parallaxes.c.adopted == 1))
        .astropy()
    )
    assert len(t) == 171, f"found {len(t)} adopted parallax entries for {ref}"


def test_suar22_ingest(db):
    ref_list = ["Suar22"]
    ref = "Suar22"

    t = (
        db.query(db.Publications)
        .filter(db.Publications.c.reference.in_(ref_list))
        .astropy()
    )
    if len(ref_list) != len(t):
        missing_ref = list(set(ref_list) - set(t["name"]))
        assert len(ref_list) == len(t), f"Missing references: {missing_ref}"

    # Check DOI and Bibcode values are correctly set for new references added
    reference_verifier(t, "Suar22", "2022MNRAS.513.5701S", "10.1093/mnras/stac1205")

    # Test for Suar22 spectra added
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 112, f"found {len(t)} spectra entries for {ref}"


@pytest.mark.parametrize(
    ("ref", "n_sources"),
    [
        ("Roja12", 1),
        ("Burg24", 1),
        ("Zhan18.2054", 32),
        ("Lodi17", 28),
        ("Lupi08", 6),
    ],
)
def test_bones_refs(db, ref, n_sources):
    # Test to verify subdwoarf sources added by the Bones Archive
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == n_sources, f"found {len(t)} reference entries for {ref}"
