# Script to add new references to the database
# https://github.com/SIMPLE-AstroDB/SIMPLE-db/issues/144
import requests
from io import BytesIO
import logging
import numpy as np
import sqlalchemy.exc

from astropy.io.votable import parse

from astrodb_scripts import AstroDBError, find_source_in_db

logger = logging.getLogger("SIMPLE")


def fetch_svo(telescope, instrument, filter_name):
    url = (
        f"http://svo2.cab.inta-csic.es/svo/theory/fps3/fps.php?ID="
        f"{telescope}/{instrument}.{filter_name}"
    )
    r = requests.get(url)

    if r.status_code != 200:
        print(f"Error retrieving {url}")
        return

    # Parse VOTable contents
    content = BytesIO(r.content)
    votable = parse(content)

    # Get Filtter ID
    filter_id = votable.get_field_by_id("filterID").value.split("/")[1]

    # Get effective wavelength and FWHM
    eff_wave = votable.get_field_by_id("WavelengthEff").value
    fwhm = votable.get_field_by_id("FWHM").value

    return filter_id, eff_wave, fwhm


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
        logging.info(
            f"Added filter {filter_id} with effective wavelength {eff_wave}, "
            f"FWHM {fwhm}, and UCD {ucd}."
        )
    except Exception as e:
        msg = str(e)
        raise AstroDBError(msg)


def ingest_photometry(
    db,
    source,
    band,
    magnitude,
    magnitude_error,
    reference,
    telescope=None,
    epoch=None,
    comments=None,
    raise_error=True,
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
                "The reference may not exist in the Publications table. "
                "Add it with add_publication function."
            )
            logger.error(msg)
            raise AstroDBError(msg)

    logger.info(f"Added photometry measurement: {photometry_data} \n")

    return flags


def assign_ucd(eff_wave):
    if 3000 < eff_wave < 4000:
        ucd = "em.opt.U"
    elif 4000 < eff_wave < 5000:
        ucd = "em.opt.B"
    elif 5000 < eff_wave < 6000:
        ucd = "em.opt.V"
    elif 6000 < eff_wave < 7500:
        ucd = "em.opt.R"
    elif 7500 < eff_wave < 10000:
        ucd = "em.opt.I"
    elif 10000 < eff_wave < 15000:
        ucd = "em.IR.J"
    elif 15000 < eff_wave < 20000:
        ucd = "em.IR.H"
    elif 20000 < eff_wave < 30000:
        ucd = "em.IR.K"
    elif 30000 < eff_wave < 40000:
        ucd = "em.IR.3-4um"
    elif 40000 < eff_wave < 80000:
        ucd = "em.IR.4-8um"
    elif 80000 < eff_wave < 150000:
        ucd = "em.IR.8-15um"
    elif 150000 < eff_wave < 300000:
        ucd = "em.IR.15-30um"
    else:
        ucd = None

    return ucd
