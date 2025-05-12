import numpy as np
import re
import logging
from sqlalchemy import and_
import sqlalchemy.exc
from astrodb_utils import AstroDBError
from astrodb_utils.sources import find_source_in_db


__all__ = [
    "ingest_spectral_type",
    "convert_spt_string_to_code",
    "convert_spt_code_to_string",
]

logger = logging.getLogger("astrodb_utils.spectral_types")
# Sets up a child of the "astrodb_utils" logger, once moved to astrodb_utils, change to __name__

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
    raise_error: bool = True,
):
    """
    Script to ingest spectral types

    Parameters
    ----------
    db: astrodbkit.astrodb.Database
        Database object created by astrodbkit
    source: str
        Name of source. Constrained by the Sources table

    spectral_type_string: str
        Spectral Type of source

    spectral_type_error: str, optional
        Spectral Type Error of source

    regime: str
        String. Constrained by Regimes table

    comment: str, optional
        Comments

    reference: str
        Reference of the Spectral Type

    raise_error: bool, optional

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

    # Check for duplicates
    duplicate_check = (
        db.query(db.SpectralTypes.c.source)
        .filter(
            and_(
                db.SpectralTypes.c.source == db_name,
                db.SpectralTypes.c.regime == regime,
                db.SpectralTypes.c.reference == reference,
                db.SpectralTypes.c.spectral_type_string == spectral_type_string,
            )
        )
        .count()
    )

    if duplicate_check > 0:
        msg = f"Spectral type already in the database: {db_name}, {spectral_type_string}, {regime}, {reference}"
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
    else:
        logger.debug(
            f"No duplicate spectral types found for : {db_name}, {regime}, {reference}"
        )

    adopted = adopt_spectral_type(db, db_name, spectral_type_error)

    if spectral_type_code is None:
        spectral_type_code = convert_spt_string_to_code(spectral_type_string)

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

    logger.debug(f"Trying to insert into Spectral Types table: \n {spt_data}")

    try:
        with db.engine.connect() as conn:
            conn.execute(db.SpectralTypes.insert().values(spt_data))
            conn.commit()
        logger.info(
            f"Spectral type added: "
            f"{spt_data['source']}, {spt_data['spectral_type_string']}, "
            f"{spt_data['regime']}, {spt_data['reference']}"
        )
    except sqlalchemy.exc.IntegrityError as e:
        if (
            db.query(db.Publications)
            .filter(db.Publications.c.reference == reference)
            .count()
            == 0
        ):
            msg = f"The publication does not exist in the database: {reference}"
            msg1 = "Add it with astrodb_utils.ingest_publication function."
            logger.debug(msg + msg1)
            if raise_error:
                logger.error(msg)
                raise AstroDBError(msg + msg1)
            else:
                logger.warning(msg + msg1)
        else:
            msg = f"Spectral type ingest failed with error {e}\n"
            if raise_error:
                logger.error(msg)
                raise AstroDBError(msg)
            else:
                logger.warning(msg)

    # check that there is only one adopted measurement
    check_one_adopted_sptype(db, db_name, raise_error=raise_error)


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

    msg = f"Converted {spectral_type_string} to {spt_code}"
    logger.debug(msg)
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

    logger.debug(f"Converted: {spectral_code} -> {spt_type}")

    return spt_type


def adopt_spectral_type(db, source, spectral_type_error):
    source_spt_data = (
        db.query(db.SpectralTypes).filter(db.SpectralTypes.c.source == source).table()
    )

    # set adopted flag
    if len(source_spt_data) == 0:
        logger.debug(
            "No Spectral Type data for this source in the database, setting adopted flag to True"
        )
        return True
    elif len(source_spt_data) > 0:
        # Spectral Type Data already exists

        adopted_ind = source_spt_data["adopted"] == 1
        if sum(adopted_ind):
            old_adopted = source_spt_data[adopted_ind]
            logger.debug(f"Old adopted data: {old_adopted}")
            if (
                old_adopted["spectral_type_error"] is not None
                and spectral_type_error is not None
            ):
                if spectral_type_error < min(old_adopted["spectral_type_error"]):
                    adopted = True
                    logger.debug(f"The new spectral type's adopted flag is:, {adopted}")
                    unset_previously_adopted(db, source)
                else:
                    adopted = False
                    logger.debug(f"The new spectral type's adopted flag is: {adopted}")
                return adopted
            else:
                logger.debug(
                    "No spectral type error found, setting adopted flag to True"
                )
                unset_previously_adopted(db, source)
                return True
        else:
            logger.debug("No adopted data found, setting adopted flag to True")
            return True


def unset_previously_adopted(db, source):
    source_spt_data = (
        db.query(db.SpectralTypes).filter(db.SpectralTypes.c.source == source).table()
    )
    logger.debug(f"Pre-existing Spectral Type data: \n {source_spt_data}")

    if len(source_spt_data) == 0:
        logger.debug("No previous data for this source in the database, doing nothing")
        return
    elif len(source_spt_data) > 0:
        # Spectral Type Data already exists
        adopted_ind = source_spt_data["adopted"] == 1
        if sum(adopted_ind):
            old_adopted = source_spt_data[adopted_ind]
            with db.engine.connect() as conn:
                conn.execute(
                    db.SpectralTypes.update()
                    .where(
                        and_(
                            db.SpectralTypes.c.source == old_adopted["source"][0],
                            db.SpectralTypes.c.regime == old_adopted["regime"][0],
                            db.SpectralTypes.c.reference == old_adopted["reference"][0],
                        )
                    )
                    .values(adopted=False)
                )
                conn.commit()
        else:
            logger.debug("No previously adopted data found, doing nothing")


def check_one_adopted_sptype(db, source, raise_error=True):
    results = (
        db.query(db.SpectralTypes)
        .filter(
            and_(
                db.SpectralTypes.c.source == source,
                db.SpectralTypes.c.adopted == True,  # noqa: E712
            )
        )
        .table()
    )
    logger.debug(f"Adopted measurements for {source}:\n{results}")
    # if logger.level <= 10:
    # results.pprint_all()
    if len(results) == 1:
        logger.debug(f"One adopted measurement for {source}")
    elif len(results) > 2:
        msg = f"Multiple adopted measurements for {source}"
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
    elif len(results) == 0:
        msg = f"No adopted measurements for {source}"
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
