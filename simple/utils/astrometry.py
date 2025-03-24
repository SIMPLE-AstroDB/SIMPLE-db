import logging
from typing import Optional, Union
from sqlalchemy import and_
import sqlalchemy.exc
from simple.schema import Parallaxes
from astropy.units import Quantity
from astropy.table import Table
from astrodbkit.astrodb import Database
from astrodb_utils.sources import AstroDBError, find_source_in_db, find_publication


__all__ = [
    "ingest_parallax",
    "ingest_proper_motions",
]

logger = logging.getLogger("SIMPLE")


def ingest_parallax(
    db,
    source: str = None,
    parallax_mas: float = None,
    parallax_err_mas: float = None,
    reference: str = None,
    comment: str = None,
    raise_error: bool = True,
):
    """

    Parameters
    ----------
    db: astrodbkit.astrodb.Database
        Database object
    source: str
        source name
    parallax: float
        parallax of source in milliarcseconds
    plx_err: float
        parallax uncertainty in milliarcseconds
    reference: str
        reference for the parallax data
    comment: str
        comments
    raise_error: bool
        True: raises error when encountered
        False: does not raise error, returns flags dictionary

    Returns
    -------
    flags: dict
        'added' : bool    - true if properly ingested
        'content' : dict  - content attempted to ingest
        'message' : str   - error message

    """
    # Search for existing parallax data and determine if this is the best
    # If no previous measurement exists, set the new one to the Adopted measurement
    flags = {"added": False, "content": {}, "message": ""}
    adopted = False
    has_old_adopted = False
    source_plx_data: Table = (
        db.query(db.Parallaxes).filter(db.Parallaxes.c.source == source).table()
    )

    if source_plx_data is None or len(source_plx_data) == 0:
        # if there's no other measurements in the database,
        # set new data Adopted = True
        adopted = True
        logger.debug("No other measurement")
    elif len(source_plx_data) > 0:  # Parallax data already exists
        # check for duplicate measurement
        dupe_ind = source_plx_data["reference"] == reference
        if sum(dupe_ind):
            logger.debug(f"Duplicate measurement\n, {source_plx_data[dupe_ind]}")
            msg = "Duplicate measurement exists with same reference"
            flags["message"] = msg
            if raise_error:
                raise AstroDBError(msg)
            else:
                return flags
        else:
            logger.debug("!!! Another parallax measurement exists,")
            if logger.level == 10:
                source_plx_data.pprint_all()

        # check for previous adopted measurement and find new adopted
        adopted_ind = source_plx_data["adopted"] == 1
        if sum(adopted_ind):
            has_old_adopted = source_plx_data[adopted_ind]
            # if errors of new data are less than other measurements,
            # set Adopted = True.
            if parallax_err_mas < min(source_plx_data["parallax_error"]):
                adopted = True
            else:
                adopted = False
            logger.debug(f"The new measurement's adopted flag is:, {adopted}")
    else:
        msg = "Unexpected state"
        logger.error(msg)
        raise RuntimeError(msg)

    # Construct data to be added
    parallax_data = {
        "source": source,
        "parallax": parallax_mas,
        "parallax_error": parallax_err_mas,
        "reference": reference,
        "adopted": adopted,
        "comments": comment,
    }
    flags["content"] = parallax_data

    try:
        plx_obj = Parallaxes(**parallax_data)
        with db.session as session:
            session.add(plx_obj)
            session.commit()
        logger.info(f" Photometry added to database: {parallax_data}\n")
        flags["added"] = True

        # unset old adopted only after ingest is successful!
        if has_old_adopted and adopted:
            with db.engine.connect() as conn:
                conn.execute(
                    db.Parallaxes.update()
                    .where(
                        and_(
                            db.Parallaxes.c.source == has_old_adopted["source"][0],
                            db.Parallaxes.c.reference
                            == has_old_adopted["reference"][0],
                        )
                    )
                    .values(adopted=False)
                )
                conn.commit()
            # check that adopted flag is successfully changed
            old_adopted_data = (
                db.query(db.Parallaxes)
                .filter(
                    and_(
                        db.Parallaxes.c.source == has_old_adopted["source"][0],
                        db.Parallaxes.c.reference == has_old_adopted["reference"][0],
                    )
                )
                .table()
            )
            logger.debug("Old adopted measurement unset")
            if logger.level == 10:
                old_adopted_data.pprint_all()

        return flags
    except sqlalchemy.exc.IntegrityError as error_msg:
        flags["added"] = False
        msg = ""
        matching_sources = (
            db.query(db.Sources).filter(db.Sources.c.source == source).astropy()
        )
        if len(matching_sources) == 0:
            msg += f"Source '{source}' does not exist in Sources table. "
        matching_refs = (
            db.query(db.Publications)
            .filter(db.Publications.c.reference == reference)
            .astropy()
        )
        if len(matching_refs) == 0:
            msg += f"Reference '{reference}' does not exist in Publications table. "
        if raise_error:
            raise AstroDBError(
                f"Error during parallax ingest. {msg}Error caught: {error_msg}"
            )
        else:
            logger.warning(msg)
            flags["message"] = msg
            return flags


# PROPER MOTIONS
def ingest_proper_motions(
    db, sources, pm_ras, pm_ra_errs, pm_decs, pm_dec_errs, pm_references
):
    """

    Parameters
    ----------
    db: astrodbkit.astrodb.Database
        Database object
    sources: list[str]
        list of source names
    pm_ras: list[float]
        list of proper motions in right ascension (RA)
    pm_ra_errs: list[float]
        list of uncertanties in proper motion RA
    pm_decs: list[float]
        list of proper motions in declination (dec)
    pm_dec_errs: list[float]
        list of uncertanties in proper motion dec
    pm_references: str or list[str]
        Reference or list of references for the proper motion measurements

    Examples
    ----------
    > ingest_proper_motions(db, my_sources, my_pm_ra, my_pm_ra_unc,
    my_pm_dec, my_pm_dec_unc, my_pm_refs,
                            verbose = True)

    """

    n_sources = len(sources)

    # Convert single element input value to list
    if isinstance(pm_references, str):
        pm_references = [pm_references] * len(sources)

    input_float_values = [pm_ras, pm_ra_errs, pm_decs, pm_dec_errs]
    for i, input_value in enumerate(input_float_values):
        if isinstance(input_value, float):
            input_value = [input_value] * n_sources
            input_float_values[i] = input_value
    pm_ras, pm_ra_errs, pm_decs, pm_dec_errs = input_float_values

    n_added = 0

    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise AstroDBError(msg)
        else:
            db_name = db_name[0]

        # Search for existing proper motion data and determine if this is the best
        # If no previous measurement exists, set the new one to the Adopted measurement
        # adopted = None  # not used
        source_pm_data = (
            db.query(db.ProperMotions)
            .filter(db.ProperMotions.c.source == db_name)
            .table()
        )
        if source_pm_data is None or len(source_pm_data) == 0:
            # if there's no other measurements in the database,
            # set new data Adopted = True
            adopted = True
        elif len(source_pm_data) > 0:
            # check to see if other measurement is a duplicate of the new data
            dupe_ind = source_pm_data["reference"] == pm_references[i]
            if sum(dupe_ind):
                logger.debug(f"Duplicate measurement\n, {source_pm_data}")
                continue

            # check for previous adopted measurement
            adopted_ind = source_pm_data["adopted"] == 1
            if sum(adopted_ind):
                old_adopted = source_pm_data[adopted_ind]
            else:
                old_adopted = None

            # if errors of new data are less than other measurements, set Adopted = True.
            if pm_ra_errs[i] < min(source_pm_data["mu_ra_error"]) and pm_dec_errs[
                i
            ] < min(source_pm_data["mu_dec_error"]):
                adopted = True
                # unset old adopted if it exists
                if old_adopted:
                    with db.engine.connect() as conn:
                        conn.execute(
                            db.ProperMotions.update()
                            .where(
                                and_(
                                    db.ProperMotions.c.source
                                    == old_adopted["source"][0],
                                    db.ProperMotions.c.reference
                                    == old_adopted["reference"][0],
                                )
                            )
                            .values(adopted=False)
                        )
                        conn.commit()
            else:
                adopted = False
                # if no previous adopted measurement,
                # set adopted to the measurement with the smallest errors
                if (
                    not adopted
                    and not old_adopted
                    and min(source_pm_data["mu_ra_error"]) < pm_ra_errs[i]
                    and min(source_pm_data["mu_dec_error"]) < pm_dec_errs[i]
                ):
                    adopted_pm = (
                        db.ProperMotions.update()
                        .where(
                            and_(
                                db.ProperMotions.c.source == db_name,
                                db.ProperMotions.c.mu_ra_error
                                == min(source_pm_data["mu_ra_error"]),
                                db.ProperMotions.c.mu_dec_error
                                == min(source_pm_data["mu_dec_error"]),
                            )
                        )
                        .values(adopted=True)
                    )
                    with db.engine.connect() as conn:
                        conn.execute(adopted_pm)
                        conn.commit()
            logger.debug("!!! Another Proper motion exists")
            if logger.level == 10:
                source_pm_data.pprint_all()

        else:
            msg = "Unexpected state"
            logger.error(msg)
            raise RuntimeError(msg)

        # Construct data to be added
        pm_data = [
            {
                "source": db_name,
                "mu_ra": pm_ras[i],
                "mu_ra_error": pm_ra_errs[i],
                "mu_dec": pm_decs[i],
                "mu_dec_error": pm_dec_errs[i],
                "adopted": adopted,
                "reference": pm_references[i],
            }
        ]
        logger.debug(f"Proper motion data to add: {pm_data}")

        try:
            with db.engine.connect() as conn:
                conn.execute(db.ProperMotions.insert().values(pm_data))
                conn.commit()
            n_added += 1
        except sqlalchemy.exc.IntegrityError:
            msg = (
                "The source may not exist in Sources table.\n"
                "The proper motion reference may not exist in Publications table. "
                "Add it with add_publication function. \n"
                "The proper motion measurement may be a duplicate."
            )
            logger.error(msg)
            raise AstroDBError(msg)

        updated_source_pm_data = (
            db.query(db.ProperMotions)
            .filter(db.ProperMotions.c.source == db_name)
            .table()
        )
        logger.info("Updated proper motion data:")
        if logger.level == 20:  # Info = 20, Debug = 10
            updated_source_pm_data.pprint_all()

    return


def ingest_radial_velocity(
    db: Database,
    *,
    source: str = None,
    rv: Union[Quantity, float] = None,
    rv_err: Optional[Union[float, Quantity]] = None,
    reference: str = None,
    raise_error: bool = True,
):
    """

    Parameters
    ----------
    db: astrodbkit.astrodb.Database
        Database object
    source: str
        source name
    rv: float or str
        radial velocity of the sources
        if not a Quantity, assumed to be in km/s
    rv_err: float or str
        radial velocity uncertainty
        if not a Quantity, assumed to be in km/s
    reference: str
        reference for the radial velocity data
    raise_error: bool
        If True, raise errors. If False, log an error and return.

    Returns
    -------
    flags: dict
        'added' : bool
        'skipped' : bool

    Examples
    ----------
    > ingest_radial_velocity(db, my_source, rv=my_rv, rv_err=my_rv_unc,
                             reference=my_rv_ref,
                             raise_error = False)

    """

    flags = {"added": False, "skipped": False}

    # Find the source in the database, make sure there's only one match
    db_name = find_source_in_db(db, source)
    if len(db_name) != 1:
        msg = f"No unique source match for {source} in the database"
        flags["skipped"] = True
        logger.error(msg)
        if raise_error:
            raise AstroDBError(msg)
        else:
            return flags
    else:
        db_name = db_name[0]

    # Make sure the publication is in the database
    pub_check = find_publication(db, reference=reference)
    if pub_check[0]:
        msg = f"Reference found: {pub_check[1]}."
        logger.info(msg)
    if not pub_check[0]:
        flags["skipped"] = True
        msg = f"Reference {reference} not found in Publications table."
        if raise_error:
            logger.error(msg)
            raise AstroDBError(msg)
        else:
            logger.warning(msg)
            return flags

    # Search for existing radial velocity data and determine if this is the best
    # If no previous measurement exists, set the new one to the Adopted measurement
    adopted = None
    source_rv_data: Table = (
        db.query(db.RadialVelocities)
        .filter(db.RadialVelocities.c.source == db_name)
        .table()
    )

    if source_rv_data is None or len(source_rv_data) == 0:
        # if there's no other measurements in the database, set new data Adopted = True
        adopted = True
        logger.debug("No other measurement")
    elif len(source_rv_data) > 0:  # Radial Velocity data already exists
        # check for duplicate measurement
        dupe_ind = source_rv_data["reference"] == reference
        if sum(dupe_ind):
            msg = f"Duplicate radial velocity measurement\n, {source_rv_data[dupe_ind]}"
            logger.warning(msg)
            flags["skipped"] = True
            if raise_error:
                raise AstroDBError(msg)
            else:
                return flags
        else:
            msg = "!!! Another Radial Velocity measurement exists,"
            logger.warning(msg)
            if logger.level == 10:
                source_rv_data.pprint_all()

            # check for previous adopted measurement and find new adopted
            adopted_ind = source_rv_data["adopted"] == 1
            if sum(adopted_ind):
                old_adopted = source_rv_data[adopted_ind]
                # if errors of new data are less than other measurements,
                # set Adopted = True.
                if rv_err < min(source_rv_data["radial_velocity_error"]):
                    adopted = True

                    # unset old adopted
                    if old_adopted:
                        db.RadialVelocities.update().where(
                            and_(
                                db.RadialVelocities.c.source
                                == old_adopted["source"][0],
                                db.RadialVelocities.c.reference
                                == old_adopted["reference"][0],
                            )
                        ).values(adopted=False).execute()
                        # check that adopted flag is successfully changed
                        old_adopted_data = (
                            db.query(db.RadialVelocities)
                            .filter(
                                and_(
                                    db.RadialVelocities.c.source
                                    == old_adopted["source"][0],
                                    db.RadialVelocities.c.reference
                                    == old_adopted["reference"][0],
                                )
                            )
                            .table()
                        )
                        logger.debug("Old adopted measurement unset")
                        if logger.level == 10:
                            old_adopted_data.pprint_all()

                logger.debug(f"The new measurement's adopted flag is:, {adopted}")
            else:
                msg = "Unexpected state"
                logger.error(msg)
                raise RuntimeError(msg)

    # Construct data to be added
    radial_velocity_data = [
        {
            "source": db_name,
            "radial_velocity_km_s": rv,
            "radial_velocity_error_km_s": rv_err,
            "reference": reference,
            "adopted": adopted,
        }
    ]

    logger.debug(f"{radial_velocity_data}")

    try:
        with db.engine.connect() as conn:
            conn.execute(db.RadialVelocities.insert().values(radial_velocity_data))
            conn.commit()
        flags["added"] = True
        msg = f"Radial Velocity added to database: \n {radial_velocity_data}"
        logger.debug(msg)
    except sqlalchemy.exc.IntegrityError:
        flags["skipped"] = True
        msg = (
            "The source may not exist in Sources table.\n"
            "The Radial Velocity reference may not exist in Publications table. "
            "Add it with add_publication function. \n"
            "The radial velocity measurement may be a duplicate."
        )
        logger.error(msg)
        raise AstroDBError(msg)

    return flags
