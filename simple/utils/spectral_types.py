import numpy as np
import re
import logging
from sqlalchemy import and_
import sqlalchemy.exc
from astrodb_scripts import (
    AstroDBError,
    find_source_in_db,
)


__all__ = [
    "ingest_spectral_types",
    "convert_spt_string_to_code",
    "convert_spt_code_to_string_to_code",
]

logger = logging.getLogger("SIMPLE")


def ingest_spectral_types(
    db,
    sources,
    spectral_types,
    references,
    regimes,
    spectral_type_error=None,
    comments=None,
):
    """
    Script to ingest spectral types
    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    sources: str or list[str]
        Names of sources
    spectral_types: str or list[strings]
        Spectral Types of sources
    spectral_type_error: str or list[strings], optional
        Spectral Type Errors of sources
    regimes: str or list[str]
        List or string
    comments: list[strings], optional
        Comments
    references: str or list[strings]
        Reference of the Spectral Type
    Returns
    -------

    None

    """

    n_sources = len(sources)

    # Convert single element input value to list
    input_values = [
        sources,
        spectral_types,
        spectral_type_error,
        regimes,
        comments,
        references,
    ]
    for i, input_value in enumerate(input_values):
        if input_value is None:
            input_values[i] = [None] * n_sources
        elif isinstance(input_value, str):
            input_values[i] = [input_value] * n_sources
        # Convert single element input value to list
    (
        sources,
        spectral_types,
        spectral_type_error,
        regimes,
        comments,
        references,
    ) = input_values

    n_added = 0
    n_skipped = 0

    logger.info(f"Trying to add {n_sources} spectral types")

    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)
        # Spectral Type data is in the database

        if len(db_name) != 1:
            msg = (
                f"No unique source match for {source} in the database "
                f"(with SpT: {spectral_types[i]} from {references[i]})"
            )
            raise AstroDBError(msg)
        else:
            db_name = db_name[0]

        adopted = None
        source_spt_data = (
            db.query(db.SpectralTypes)
            .filter(db.SpectralTypes.c.source == db_name)
            .table()
        )

        if source_spt_data is None or len(source_spt_data) == 0:
            adopted: True
            logger.debug("No Spectral Type data for this source in the database")
        elif len(source_spt_data) > 0:
            # Spectral Type Data already exists
            dupe_ind = source_spt_data["reference"] == references[i]
            if sum(dupe_ind):
                logger.debug(f"Duplicate measurement\n, {source_spt_data[dupe_ind]}")
            else:
                logger.debug("Another Spectral Type exists,")
                if logger.level == 10:
                    source_spt_data.pprint_all()

            adopted_ind = source_spt_data["adopted"] == 1
            if sum(adopted_ind):
                old_adopted = source_spt_data[adopted_ind]
                if spectral_type_error[i] < min(source_spt_data["spectral_type_error"]):
                    adopted = True

                    if old_adopted:
                        with db.engine.connect() as conn:
                            conn.execute(
                                db.SpectralTypes.update()
                                .where(
                                    and_(
                                        db.SpectralTypes.c.source
                                        == old_adopted["source"][0],
                                        db.SpectralTypes.c.reference
                                        == old_adopted["reference"][0],
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
                                    db.SpectralTypes.c.source
                                    == old_adopted["source"][0],
                                    db.SpectralTypes.c.reference
                                    == old_adopted["reference"][0],
                                )
                            )
                            .table()
                        )
                        logger.debug("Old adopted measurement unset")
                        if logger.level == 10:
                            old_adopted_data.pprint_all()

                logger.debug(f"The new spectral type's adopted flag is:, {adopted}")
        else:
            msg = "Unexpected state"
            logger.error(msg)
            raise RuntimeError

        # Convert the spectral type string to code
        spectral_type_code = convert_spt_string_to_code(spectral_types[i])[0]
        msg = f"Converted {spectral_types[i]} to {spectral_type_code}"
        logger.debug(msg)

        # Construct the data to be added
        spt_data = [
            {
                "source": db_name,
                "spectral_type_string": spectral_types[i],
                "spectral_type_code": spectral_type_code,
                "spectral_type_error": spectral_type_error[i],
                "regime": regimes[i],
                "adopted": adopted,
                "comments": comments[i],
                "reference": references[i],
            }
        ]

        # Check if the entry already exists; if so: skip adding it
        check = (
            db.query(db.SpectralTypes.c.source)
            .filter(
                and_(
                    db.SpectralTypes.c.source == db_name,
                    db.SpectralTypes.c.regime == regimes[i],
                    db.SpectralTypes.c.reference == references[i],
                )
            )
            .count()
        )
        if check == 1:
            n_skipped += 1
            logger.info(
                f"Spectral type for {db_name} already in the database: skipping insert "
                f"{spt_data}"
            )
            continue

        logger.debug(f"Trying to insert {spt_data} into Spectral Types table ")
        try:
            with db.engine.connect() as conn:
                conn.execute(db.SpectralTypes.insert().values(spt_data))
                conn.commit()
            n_added += 1
            msg = f"Added {str(spt_data)}"
            logger.debug(msg)
        except sqlalchemy.exc.IntegrityError as e:
            if (
                db.query(db.Publications)
                .filter(db.Publications.c.reference == references[i])
                .count()
                == 0
            ):
                msg = f"The publication {references[i]} does not exist in the database"
                msg1 = "Add it with ingest_publication function."
                logger.debug(msg + msg1)
                raise AstroDBError(msg)
            elif "NOT NULL constraint failed: SpectralTypes.regime" in str(e):
                msg = f"The regime was not provided for {source}"
                logger.error(msg)
                raise AstroDBError(msg)
            else:
                msg = "Other error\n"
                logger.error(msg)
                raise AstroDBError(msg)

    msg = f"Spectral types added: {n_added} \n" f"Spectral Types skipped: {n_skipped}"
    logger.info(msg)


def convert_spt_string_to_code(spectral_types):
    """
    normal tests: M0, M5.5, L0, L3.5, T0, T3, T4.5, Y0, Y5, Y9.
    weird TESTS: sdM4, ≥Y4, T5pec, L2:, L0blue, Lpec, >L9, >M10, >L, T, Y
    digits are needed in current implementation.
    :param spectral_types:
    :return:
    """
    if isinstance(spectral_types, str):
        spectral_types = [spectral_types]

    spectral_type_codes = []
    for spt in spectral_types:
        logger.debug(f"Trying to convert: `{spt}`")
        spt_code = np.nan
        if spt == "":
            spectral_type_codes.append(spt_code)
            logger.debug("Appended NAN")
            continue
        if spt == "null":
            spt_code = 0
            spectral_type_codes.append(spt_code)
            logger.debug("Appended Null")
            continue
        # identify main spectral class, loop over any prefix text to identify MLTY
        for i, item in enumerate(spt):
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
        if re.search("\d*\.?\d+", spt[i + 1 :]) is None:
            spt_code = spt_code
        else:
            spt_code += float(re.findall("\d*\.?\d+", spt[i + 1 :])[0])

        spectral_type_codes.append(spt_code)
    return spectral_type_codes


def convert_spt_code_to_string_to_code(spectral_codes, decimals=1):
    """
    Convert spectral type codes to string values

    Parameters
    ----------
    spectral_codes : list[float]
        List of spectral type codes

    Returns
    -------
    spectral_types : list[str]
        List of spectral types
    """
    if isinstance(spectral_codes, float):
        spectral_codes = [spectral_codes]

    spectral_types = []
    for spt in spectral_codes:
        spt_type = ""

        # Identify major type
        if 60 <= spt < 70:
            spt_type = "M"
        elif 70 <= spt < 80:
            spt_type = "L"
        elif 80 <= spt < 90:
            spt_type = "T"
        elif 90 <= spt < 100:
            spt_type = "Y"

        # Numeric part of type
        format = f".{decimals}f"
        spt_type = f"{spt_type}{spt % 10:{format}}"
        logger.debug(f"Converting: {spt} -> {spt_type}")

        spectral_types.append(spt_type)

    return spectral_types
