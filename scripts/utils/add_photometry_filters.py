# Script to add new references to the database
# https://github.com/SIMPLE-AstroDB/SIMPLE-db/issues/144
import requests
from io import BytesIO
import logging

from astropy.io.votable import parse

from astrodb_scripts import AstroDBError

logger = logging.getLogger("SIMPLE")

def fetch_svo(telescope, instrument, filter_name):
    url = (
        f"http://svo2.cab.inta-csic.es/svo/theory/fps3/fps.php?ID="
        f"{telescope}/{instrument}.{filter_name}"
    )
    r = requests.get(url)

    if r.status_code != 200:
        print(f'Error retrieving {url}')
        return

    # Parse VOTable contents
    content = BytesIO(r.content)
    votable = parse(content)

    # Get Filtter ID
    filter_id = votable.get_field_by_id("filterID").value.split("/")[1]

    # Get effective wavelength and FWHM
    eff_wave = votable.get_field_by_id('WavelengthEff').value
    fwhm = votable.get_field_by_id('FWHM').value

    return filter_id, eff_wave, fwhm


def add_photometry_filter(
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
            f"Added filter {filter_id} with effective wavelength {eff_wave}, FWHM {fwhm}, and UCD {ucd}."
        )
    except Exception as e:
        msg = str(e)
        raise AstroDBError(msg)
