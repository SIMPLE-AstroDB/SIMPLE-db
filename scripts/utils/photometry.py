import requests
from io import BytesIO
import logging
import numpy as np
import sqlalchemy.exc
from typing import Optional
import astropy.units as u
from astropy.io.votable import parse

from astrodb_scripts import AstroDBError, find_source_in_db

logger = logging.getLogger("AstroDB")

__all__ = ["ingest_photometry", "ingest_photometry_filter", "fetch_svo", "assign_ucd"]


def ingest_photometry(
    db,
    *,
    source: str = None,
    band: str = None,
    magnitude: float = None,
    magnitude_error: float = None,
    reference: str = None,
    telescope: Optional[str] = None,
    epoch: Optional[float] = None,
    comments: Optional[str] = None,
    raise_error: bool = True,
):
    """
    TODO: Write Docstring

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
    sources: list[str]
    bands: str or list[str]
    magnitudes: list[float]
    magnitude_errors: list[float]
    reference: str or list[str]
    telescope: str or list[str]
    epoch: list[float], optional
    comments: list[str], optional
    raise_error: bool, optional
        True (default): Raise an error if a source cannot be ingested
        False: Log a warning but skip sources which cannot be ingested

    Returns
    -------

    """
    flags = {"added": False}

    db_name = find_source_in_db(db, source)

    if len(db_name) != 1:
        msg = f"No unique source match for {source} in the database"
        raise AstroDBError(msg)
    else:
        db_name = db_name[0]

    # if the uncertainty is masked, don't ingest anything
    if isinstance(magnitude_error, np.ma.core.MaskedConstant):
        mag_error = None
    else:
        mag_error = str(magnitude_error)

    # Construct data to be added
    photometry_data = [
        {
            "source": db_name,
            "band": band,
            "magnitude": str(
                magnitude
            ),  # Convert to string to maintain significant digits
            "magnitude_error": mag_error,
            "telescope": telescope,
            "epoch": epoch,
            "comments": comments,
            "reference": reference,
        }
    ]
    logger.debug(f"Photometry data: {photometry_data}")

    try:
        with db.engine.connect() as conn:
            conn.execute(db.Photometry.insert().values(photometry_data))
            conn.commit()
        flags["added"] = True
        logger.info(f"Photometry measurement added: \n" f"{photometry_data}")
    except sqlalchemy.exc.IntegrityError as e:
        if "UNIQUE constraint failed:" in str(e):
            msg = "The measurement may be a duplicate."
            if raise_error:
                logger.error(msg)
                raise AstroDBError(msg)
            else:
                logger.warning(msg)
        else:
            msg = (
                "The source may not exist in Sources table.\n"
                "The band may not exist in the PhotometryFilters table.\n"
                "The reference may not exist in the Publications table. "
                "Add it with add_publication function."
            )
            if raise_error:
                logger.error(msg)
                raise AstroDBError(msg)
            else:
                logger.warning(msg)

    return flags


def ingest_photometry_filter(
    db, *, telescope=None, instrument=None, filter_name=None, ucd=None
):
    """
    Add a new photometry filter to the database
    """
    # Fetch existing telescopes, add if missing
    existing = (
        db.query(db.Telescopes).filter(db.Telescopes.c.telescope == telescope).table()
    )
    if len(existing) == 0:
        with db.engine.connect() as conn:
            conn.execute(db.Telescopes.insert().values({"telescope": telescope}))
            conn.commit()

    # Fetch existing instruments, add if missing
    existing = (
        db.query(db.Instruments)
        .filter(db.Instruments.c.instrument == instrument)
        .table()
    )
    if len(existing) == 0:
        with db.engine.connect() as conn:
            conn.execute(db.Instruments.insert().values({"instrument": instrument}))
            conn.commit()

    # Get data from SVO
    filter_id, eff_wave, fwhm = fetch_svo(telescope, instrument, filter_name)

    if ucd is None:
        ucd = assign_ucd(eff_wave)

    # Add the filter
    try:
        with db.engine.connect() as conn:
            conn.execute(
                db.PhotometryFilters.insert().values(
                    {
                        "band": filter_id,
                        "ucd": ucd,
                        "effective_wavelength": eff_wave,
                        "width": fwhm,
                    }
                )
            )
            conn.commit()
        logger.info(
            f"Added filter {filter_id} with effective wavelength {eff_wave}, "
            f"FWHM {fwhm}, and UCD {ucd}."
        )
    except sqlalchemy.exc.IntegrityError as e:
        if "UNIQUE constraint failed:" in str(e):
            msg = str(e) + f"Filter {filter_id} already exists in the database."
            raise AstroDBError(msg)
        else:
            msg = str(e) + f"Error adding filter {filter_id}."
            raise AstroDBError(msg)
    except Exception as e:
        msg = str(e)
        raise AstroDBError(msg)


def fetch_svo(telescope: str = None, instrument: str = None, filter_name: str = None):
    """
    Fetch photometry filter information from the SVO Filter Profile Service
    http://svo2.cab.inta-csic.es/theory/fps/

    Could use better error handling when instrument name or filter name is not found

    Parameters
    ----------
    telescope: str
        Telescope name
    instrument: str
        Instrument name
    filter_name: str
        Filter name

    Returns
    -------
    filter_id: str
        Filter ID
    eff_wave: Quantity
        Effective wavelength
    fwhm: Quantity
        Full width at half maximum (FWHM)


    Raises
    ------
    AstroDBError
        If the SVO URL is not reachable or the filter information is not found
    KeyError
        If the filter information is not found in the VOTable
    """
    url = (
        f"http://svo2.cab.inta-csic.es/svo/theory/fps3/fps.php?ID="
        f"{telescope}/{instrument}.{filter_name}"
    )
    r = requests.get(url)

    if r.status_code != 200:
        msg = f"Error retrieving {url}. Status code: {r.status_code}"
        logger.error(msg)
        raise AstroDBError(msg)

    # Parse VOTable contents
    content = BytesIO(r.content)
    votable = parse(content)

    # Get Filter ID
    try:
        filter_id = votable.get_field_by_id("filterID").value
    except KeyError:
        msg = f"Filter {telescope}, {instrument}, {filter_name} not found in SVO."
        raise AstroDBError(msg)

    # Get effective wavelength and FWHM
    eff_wave = votable.get_field_by_id("WavelengthEff")
    fwhm = votable.get_field_by_id("FWHM")

    if eff_wave.unit == "AA" and fwhm.unit == "AA":
        eff_wave = eff_wave.value * u.Angstrom
        fwhm = fwhm.value * u.Angstrom
    else:
        msg = f"Wavelengths from SVO may not be Angstroms as expected: {eff_wave.unit}"
        raise AstroDBError(msg)

    logger.debug(
        f"Found in SVO: "
        f"Filter {filter_id} has effective wavelength {eff_wave} and "
        f"FWHM {fwhm}."
    )

    return filter_id, eff_wave, fwhm


def assign_ucd(eff_wave):
    """
    Assign a Unified Content Descriptors (UCD) to a photometry filter based on its effective wavelength
    UCDs are from the UCD1+ controlled vocabulary
    https://www.ivoa.net/documents/UCD1+/20200212/PEN-UCDlist-1.4-20200212.html#tth_sEcB

    Parameters
    ----------
    eff_wave: float
        Effective wavelength in Angstroms

    Returns
    -------
    ucd: str
        UCD string

    """
    if 3000 < eff_wave <= 4000:
        ucd = "em.opt.U"
    elif 4000 < eff_wave <= 5000:
        ucd = "em.opt.B"
    elif 5000 < eff_wave <= 6000:
        ucd = "em.opt.V"
    elif 6000 < eff_wave <= 7500:
        ucd = "em.opt.R"
    elif 7500 < eff_wave <= 10000:
        ucd = "em.opt.I"
    elif 10000 < eff_wave <= 15000:
        ucd = "em.IR.J"
    elif 15000 < eff_wave <= 20000:
        ucd = "em.IR.H"
    elif 20000 < eff_wave <= 30000:
        ucd = "em.IR.K"
    elif 30000 < eff_wave <= 40000:
        ucd = "em.IR.3-4um"
    elif 40000 < eff_wave <= 80000:
        ucd = "em.IR.4-8um"
    elif 80000 < eff_wave <= 150000:
        ucd = "em.IR.8-15um"
    elif 150000 < eff_wave <= 300000:
        ucd = "em.IR.15-30um"
    else:
        ucd = None

    return ucd
