import numpy as np
import re
import logging
from sqlalchemy import and_
import sqlalchemy.exc
from simple.schema import SpectralTypes
from astrodb_utils import (
    AstroDBError,
    find_source_in_db,
)


__all__ = [
    "ingest_spectral_type",
    "convert_spt_string_to_code",
    "convert_spt_code_to_string_to_code",
]

logger = logging.getLogger("SIMPLE")


def ingest_spectral_type(
    db,
    source: str = None,
    *,
    spectral_type_string: str = None,
    spectral_type_code: float = None,
    spectral_type_error: float = None,
    regime: str = None,
    photometric: bool = False,
    comments: str = None,
    reference: str = None,
):
    """
    Script to ingest spectral types
    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    source: str
        Name of source
    spectral_type: str
        Spectral Type of source
    spectral_type_error: str, optional
        Spectral Type Error of source
    regime: str
        String
    comment: str, optional
        Comments
    reference: str
        Reference of the Spectral Type

    Returns
    -------

    None
    """

    db_name = find_source_in_db(db, source)

    if len(db_name) != 1:
        msg = f"No unique source match for {source} in the database "
        raise AstroDBError(msg)
    else:
        db_name = db_name[0]

    adopted = False
    old_adopted = None
    source_spt_data = (
        db.query(db.SpectralTypes).filter(db.SpectralTypes.c.source == db_name).table()
    )

    # set adopted flag
    if source_spt_data is None or len(source_spt_data) == 0:
        adopted = True
        logger.debug("No Spectral Type data for this source in the database")
    elif len(source_spt_data) > 0:
        # Spectral Type Data already exists
        dupe_ind = source_spt_data["reference"] == reference
        if sum(dupe_ind):
            logger.debug(f"Duplicate measurement\n, {source_spt_data[dupe_ind]}")
        else:
            logger.debug("Another Spectral Type exists,")
            if logger.level == 10:
                source_spt_data.pprint_all()

        adopted_ind = source_spt_data["adopted"] == 1
        if sum(adopted_ind):
            old_adopted = source_spt_data[adopted_ind]
            print("spt_error:", spectral_type_error)
            print("source spt data:", source_spt_data["spectral_type_error"])
            if (
                spectral_type_error is not None
                and source_spt_data["spectral_type_error"] is not None
            ):
                if spectral_type_error < min(source_spt_data["spectral_type_error"]):
                    adopted = True
                logger.debug(f"The new spectral type's adopted flag is:, {adopted}")
    else:
        msg = "Unexpected state"
        logger.error(msg)
        raise RuntimeError

    if spectral_type_code is None:
        spectral_type_code = convert_spt_string_to_code(spectral_type_string)
        msg = f"Converted {spectral_type_string} to {spectral_type_code}"
        logger.debug(msg)

    # Construct the data to be added
    spt_data = {
        "source": db_name,
        "spectral_type_string": spectral_type_string,
        "spectral_type_code": spectral_type_code,
        "spectral_type_error": spectral_type_error,
        "regime": regime,
        "adopted": adopted,
        "photometric": photometric,
        "comments": comments,
        "reference": reference,
    }

    check = (
        db.query(db.SpectralTypes.c.source)
        .filter(
            and_(
                db.SpectralTypes.c.source == db_name,
                db.SpectralTypes.c.regime == regime,
                db.SpectralTypes.c.reference == reference,
            )
        )
        .count()
    )
    if check == 1:
        msg = f"Spectral type for {db_name} already in the database"
        raise AstroDBError(msg)

    logger.debug(f"Trying to insert {spt_data} into Spectral Types table ")

    try:
        spt_obj = SpectralTypes(**spt_data)
        with db.session as session:
            session.add(spt_obj)
            session.commit()
        logger.info(f"Spectral type added to database: {spt_data}\n")

        # unset old adopted only after ingest is successful!
        if adopted and old_adopted is not None:
            with db.engine.connect() as conn:
                conn.execute(
                    db.SpectralTypes.update()
                    .where(
                        and_(
                            db.SpectralTypes.c.source == old_adopted["source"][0],
                            db.SpectralTypes.c.reference == old_adopted["reference"][0],
                        )
                    )
                    .values(adopted=False)
                )
                conn.commit()
            # check that adopted flag is successfully changed
            old_adopted_data = (
                db.query(db.SpectralTypes)
                .filter(
                    and_(
                        db.SpectralTypes.c.source == old_adopted["source"][0],
                        db.SpectralTypes.c.reference == old_adopted["reference"][0],
                    )
                )
                .table()
            )
            logger.debug("Old adopted measurement unset")
            if logger.level == 10:
                old_adopted_data.pprint_all()
    except sqlalchemy.exc.IntegrityError as e:
        if (
            db.query(db.Publications)
            .filter(db.Publications.c.reference == reference)
            .count()
            == 0
        ):
            msg = f"The publication does not exist in the database: {reference}"
            msg1 = "Add it with ingest_publication function."
            logger.debug(msg + msg1)
            raise AstroDBError(msg)
        elif "NOT NULL constraint failed: SpectralTypes.regime" in str(e):
            msg = f"The regime was not provided for {source}"
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            msg = f"Spectral type ingest failed with error {e}\n"
            logger.error(msg)
            raise AstroDBError(msg)


def convert_spt_string_to_code(spectral_type_string):
    """
    normal tests: M0, M5.5, L0, L3.5, T0, T3, T4.5, Y0, Y5, Y9.
    weird TESTS: sdM4, ≥Y4, T5pec, L2:, L0blue, Lpec, >L9, >M10, >L, T, Y
    digits are needed in current implementation.
    :param spectral_types:
    :return:
    """

    logger.debug(f"Trying to convert: `{spectral_type_string}`")
    spt_code = np.nan
    if spectral_type_string == "":
        logger.debug("Empty spectral_type_string")
        return None
    if spectral_type_string == "null":
        return None
    # identify main spectral class, loop over any prefix text to identify MLTY
    for i, item in enumerate(spectral_type_string):
        if item == "M":
            spt_code = 60
            break
        elif item == "L":
            spt_code = 70
            break
        elif item == "T":
            spt_code = 80
            break
        elif item == "Y":
            spt_code = 90
            break
        else:  # only trigger if not MLTY
            i = 0
    # find integer or decimal subclass and add to spt_code
    if re.search(r"\d*\.?\d+", spectral_type_string[i + 1 :]) is None:
        spt_code = spt_code
    else:
        spt_code += float(re.findall(r"\d*\.?\d+", spectral_type_string[i + 1 :])[0])

    return spt_code


def convert_spt_code_to_string(spectral_code, decimals=1):
    """
    Convert spectral type codes to string values

    Parameters
    ----------
    spectral_code : float
        A spectral type code

    decimals : int
        Number of decimal places to include in the spectral type string

    Returns
    -------
    spectral_type_string : str
        spectral type string
    """
    spt_type = ""

    # Identify major type
    if 60 <= spectral_code < 70:
        spt_type = "M"
    elif 70 <= spectral_code < 80:
        spt_type = "L"
    elif 80 <= spectral_code < 90:
        spt_type = "T"
    elif 90 <= spectral_code < 100:
        spt_type = "Y"

    # Numeric part of type
    format = f".{decimals}f"
    spt_type = f"{spt_type}{spectral_code% 10:{format}}"
    logger.debug(f"Converting: {spectral_code} -> {spt_type}")

    return spt_type
