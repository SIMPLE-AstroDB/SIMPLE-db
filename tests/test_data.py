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


def test_discovery_references(db):
    """
    Values found with this SQL query:
        SELECT reference, count(*)
        FROM Sources
        GROUP BY reference
        ORDER By 2 DESC

    """

    ref = "Schm10.1808"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 208, f"found {len(t)} discovery reference entries for {ref}"

    ref = "West08"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 192, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Reid08.1290"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 206, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Cruz03"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 165, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Maro15"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 113, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Best15"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 101, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Kirk11"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 100, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Mace13.6"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 93, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Burn13"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 69, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Gagn15.33"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 68, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Chiu06"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 62, f"found {len(t)} discovery reference entries for {ref}"

    ref = "DayJ13"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 61, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Kirk10"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 56, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Cruz07"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 91, f"found {len(t)} discovery reference entries for {ref}"

    ref = "Roth"
    t = db.query(db.Sources).filter(db.Sources.c.reference == ref).astropy()
    assert len(t) == 83, f"found {len(t)} discovery reference entries for {ref}"


def test_proper_motion_refs(db):
    """
    Values found with this SQL query:
        SELECT reference, count(*)
        FROM ProperMotions
        GROUP BY reference
        ORDER By 2 DESC

    from sqlalchemy import func
    proper_motion_mearsurements = db.query(ProperMotions.reference, func.count(
        ProperMotions.reference)).\
        group_by(ProperMotions.reference).order_by(
            func.count(ProperMotions.reference).desc()).limit(20).all()
    """
    ref = "GaiaEDR3"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 1133, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "GaiaDR2"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 1076, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "Best20.257"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 348, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "Gagn15.73"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 325, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "Fahe09"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 216, f"found {len(t)} proper motion reference entries for {ref}"

    # Kirk19 tested below.

    ref = "Best15"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 120, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "Burn13"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 97, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "Dahn17"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 79, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "Jame08"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 73, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "vanL07"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 68, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "Smar18"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 68, f"found {len(t)} proper motion reference entries for {ref}"

    ref = "Schm10.1808"
    t = db.query(db.ProperMotions).filter(db.ProperMotions.c.reference == ref).astropy()
    assert len(t) == 44, f"found {len(t)} proper motion reference entries for {ref}"


def test_missions(db):
    # If 2MASS designation in Names, 2MASS photometry should exist
    stm = except_(
        select(db.Names.c.source).where(db.Names.c.other_name.like("2MASS J%")),
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("2MASS%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 259
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
        len(s) == 0
    ), f"found {len(s)} sources with Gaia designation that have no GAIA photometry"

    # If Gaia photometry, Gaia designation should be in Names
    stm = except_(
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("GAIA%")),
        select(db.Names.c.source).where(db.Names.c.other_name.like("Gaia%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 16
    ), f"found {len(s)} sources with Gaia photometry and no Gaia designation in Names"

    # If Wise designation in Names, Wise phot should exist
    stm = except_(
        select(db.Names.c.source).where(db.Names.c.other_name.like("WISE%")),
        select(db.Photometry.c.source).where(db.Photometry.c.band.like("WISE%")),
    )
    s = db.session.scalars(stm).all()
    assert (
        len(s) == 481
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


def test_spectra(db):
    regime = "optical"
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == 742, f"found {len(t)} spectra in the {regime} regime"

    regime = "nir"
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == 580, f"found {len(t)} spectra in the {regime} regime"

    regime = "mir"
    t = db.query(db.Spectra).filter(db.Spectra.c.regime == regime).astropy()
    assert len(t) == 204, f"found {len(t)} spectra in the {regime} regime"

    telescope = "IRTF"
    t = db.query(db.Spectra).filter(db.Spectra.c.telescope == telescope).astropy()
    assert len(t) == 436, f"found {len(t)} spectra from {telescope}"

    telescope = "JWST"
    instrument = "NIRSpec"
    t = (
        db.query(db.Spectra)
        .filter(
            and_(
                db.Spectra.c.telescope == telescope,
                db.Spectra.c.instrument == instrument,
            )
        )
        .astropy()
    )
    assert len(t) == 2, f"found {len(t)} spectra from {telescope}/{instrument}"

    telescope = "HST"
    instrument = "WFC3"
    t = (
        db.query(db.Spectra)
        .filter(
            and_(
                db.Spectra.c.telescope == telescope,
                db.Spectra.c.instrument == instrument,
            )
        )
        .astropy()
    )
    assert len(t) == 77, f"found {len(t)} spectra from {telescope}/{instrument}"

    ref = "Reid08.1290"
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 280, f"found {len(t)} spectra from {ref}"

    ref = "Cruz03"
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 191, f"found {len(t)} spectra from {ref}"

    ref = "Cruz18"
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 186, f"found {len(t)} spectra from {ref}"

    ref = "Cruz07"
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 158, f"found {len(t)} spectra from {ref}"

    ref = "Bard14"
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 57, f"found {len(t)} spectra from {ref}"

    ref = "Burg10.1142"
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 46, f"found {len(t)} spectra from {ref}"

    ref = "Manj20"
    t = db.query(db.Spectra).filter(db.Spectra.c.reference == ref).astropy()
    assert len(t) == 20, f"found {len(t)} spectra from {ref}"


# Test to verify existing counts of spectral types grouped by regime
@pytest.mark.parametrize(
    ("regime", "n_spectra"),
    [("optical", 1494), ("nir", 2446), ("mir", 0), ("unknown", 10)],
)
def test_spectral_types_regimes(db, regime, n_spectra):
    t = db.query(db.SpectralTypes).filter(db.SpectralTypes.c.regime == regime).astropy()
    assert len(t) == n_spectra, f"found {len(t)} spectral types in the {regime} regime"
    print(f"found {len(t)} spectral types in the {regime} regime")

# Test numbers of MLTY dwarfs
@pytest.mark.parametrize(
    ("string", "code_min", "code_max", "n_spectra"),
    [("M", 60, 70, 861), ("L", 70, 80, 2011), ("T", 80, 90, 1019), ("Y", 90, 100, 59)],
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
    assert n_spectral_types == 3950, f"found {n_spectral_types} spectral types"
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
        n_adopted_spectral_types == 88
    ), f"found {n_adopted_spectral_types} adopted spectral types"
    print(f"found {n_adopted_spectral_types} adopted spectral types")


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


def test_modeledparameters(db):
    # Test to verify existing counts of modeled parameters
    ref = "Fili15"
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.reference == ref)
        .astropy()
    )
    assert len(t) == 696, f"found {len(t)} modeled parameters with {ref} reference"

    # Test to verify log g counts
    param = "log g"
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.parameter == param)
        .astropy()
    )
    assert len(t) == 176, f"found {len(t)} modeled parameters with {param} parameter"

    # Test to verify metallicity counts
    param = "metallicity"
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.parameter == param)
        .astropy()
    )
    assert len(t) == 2, f"found {len(t)} modeled parameters with {param} parameter"

    # Test to verify radius counts
    param = "radius"
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.parameter == param)
        .astropy()
    )
    assert len(t) == 175, f"found {len(t)} modeled parameters with {param} parameter"

    # Test to verify mass counts
    param = "mass"
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.parameter == param)
        .astropy()
    )
    assert len(t) == 176, f"found {len(t)} modeled parameters with {param} parameter"

    # Test to verify T eff counts
    param = "T eff"
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.parameter == param)
        .astropy()
    )
    assert len(t) == 176, f"found {len(t)} modeled parameters with {param} parameter"

    # Test to verify Lodi22 reference counts
    ref = "Lodi22"
    t = (
        db.query(db.ModeledParameters)
        .filter(db.ModeledParameters.c.reference == ref)
        .astropy()
    )
    assert len(t) == 5, f"found {len(t)} modeled parameters with {ref} reference"


def test_radial_velocities(db):
    t = db.query(db.RadialVelocities).astropy()
    assert len(t) == 1015, f"found {len(t)} radial velociies"

    ref = "Abaz09"
    t = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.reference == ref)
        .astropy()
    )
    assert len(t) == 445, f"found {len(t)} radial velociies with {ref} reference"

    ref = "Fahe16"
    t = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.reference == ref)
        .astropy()
    )
    assert len(t) == 47, f"found {len(t)} radial velociies with {ref} reference"

    t = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.radial_velocity_error_km_s == None)
        .astropy()
    )
    assert len(t) == 89, f"found {len(t)} radial velociies with no uncertainty"

def test_companion_relations(db):
    t = db.query(db.CompanionRelationships).astropy()
    assert len(t) == 102, f"found {len(t)} companion relationships"

    ref = "Roth24"
    t = (
        db.query(db.CompanionRelationships)
        .filter(db.CompanionRelationships.c.reference == ref)
        .astropy()
    )
    assert len(t) == 89, f"found {len(t)} companion relationships with {ref} reference"