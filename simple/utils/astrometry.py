from astropy.table import Table
import logging
from sqlalchemy import and_
import sqlalchemy.exc
from astrodb_scripts import (
    AstroDBError,
    find_source_in_db,
)


__all__ = [
    "ingest_parallaxes",
    "ingest_proper_motions",
]

logger = logging.getLogger("SIMPLE")


# PARALLAXES
def ingest_parallaxes(db, sources, plxs, plx_errs, plx_refs, comments=None):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object
    sources: str or list[str]
        list of source names
    plxs: float or list[float]
        list of parallaxes corresponding to the sources
    plx_errs: float or list[float]
        list of parallaxes uncertainties
    plx_refs: str or list[str]
        list of references for the parallax data
    comments: Optional[Union[List[str], str]]

    Examples
    ----------
    > ingest_parallaxes(db, my_sources, my_plx, my_plx_unc, my_plx_refs)

    """

    if isinstance(sources, str):
        n_sources = 1
        sources = [sources]
    else:
        n_sources = len(sources)

    # Convert single element input value to list
    if isinstance(plx_refs, str):
        plx_refs = [plx_refs] * n_sources

    if isinstance(comments, str):
        comments = [comments] * n_sources
    elif comments is None:
        comments = [None] * n_sources

    input_float_values = [plxs, plx_errs]
    for i, input_value in enumerate(input_float_values):
        if isinstance(input_value, float):
            input_value = [input_value] * n_sources
            input_float_values[i] = input_value
    plxs, plx_errs = input_float_values

    n_added = 0

    # loop through sources with parallax data to ingest
    for i, source in enumerate(sources):
        db_name = find_source_in_db(db, source)

        if len(db_name) != 1:
            msg = f"No unique source match for {source} in the database"
            raise AstroDBError(msg)
        else:
            db_name = db_name[0]

        # Search for existing parallax data and determine if this is the best
        # If no previous measurement exists, set the new one to the Adopted measurement
        adopted = None
        source_plx_data: Table = (
            db.query(db.Parallaxes).filter(db.Parallaxes.c.source == db_name).table()
        )

        if source_plx_data is None or len(source_plx_data) == 0:
            # if there's no other measurements in the database,
            # set new data Adopted = True
            adopted = True
            # old_adopted = None  # not used
            logger.debug("No other measurement")
        elif len(source_plx_data) > 0:  # Parallax data already exists
            # check for duplicate measurement
            dupe_ind = source_plx_data["reference"] == plx_refs[i]
            if sum(dupe_ind):
                logger.debug(f"Duplicate measurement\n, {source_plx_data[dupe_ind]}")
                continue
            else:
                logger.debug("!!! Another parallax measurement exists,")
                if logger.level == 10:
                    source_plx_data.pprint_all()

            # check for previous adopted measurement and find new adopted
            adopted_ind = source_plx_data["adopted"] == 1
            if sum(adopted_ind):
                old_adopted = source_plx_data[adopted_ind]
                # if errors of new data are less than other measurements,
                # set Adopted = True.
                if plx_errs[i] < min(source_plx_data["parallax_error"]):
                    adopted = True

                    # unset old adopted
                    if old_adopted:
                        with db.engine.connect() as conn:
                            conn.execute(
                                db.Parallaxes.update()
                                .where(
                                    and_(
                                        db.Parallaxes.c.source
                                        == old_adopted["source"][0],
                                        db.Parallaxes.c.reference
                                        == old_adopted["reference"][0],
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
                                    db.Parallaxes.c.source == old_adopted["source"][0],
                                    db.Parallaxes.c.reference
                                    == old_adopted["reference"][0],
                                )
                            )
                            .table()
                        )
                        logger.debug("Old adopted measurement unset")
                        if logger.level == 10:
                            old_adopted_data.pprint_all()
                else:
                    adopted = False
                logger.debug(f"The new measurement's adopted flag is:, {adopted}")
        else:
            msg = "Unexpected state"
            logger.error(msg)
            raise RuntimeError(msg)

        # Construct data to be added
        parallax_data = [
            {
                "source": db_name,
                "parallax": plxs[i],
                "parallax_error": plx_errs[i],
                "reference": plx_refs[i],
                "adopted": adopted,
                "comments": comments[i],
            }
        ]

        logger.debug(f"{parallax_data}")

        try:
            with db.engine.connect() as conn:
                conn.execute(db.Parallaxes.insert().values(parallax_data))
                conn.commit()
            n_added += 1
            logger.info(f"Parallax added to database: \n " f"{parallax_data}")
        except sqlalchemy.exc.IntegrityError:
            msg = (
                "The source may not exist in Sources table.\n"
                "The parallax reference may not exist in Publications table. "
                "Add it with add_publication function. \n"
                "The parallax measurement may be a duplicate."
            )
            logger.error(msg)
            raise AstroDBError(msg)

    logger.info(f"Total Parallaxes added to database: {n_added} \n")

    return


# PROPER MOTIONS
def ingest_proper_motions(
    db, sources, pm_ras, pm_ra_errs, pm_decs, pm_dec_errs, pm_references
):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
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
