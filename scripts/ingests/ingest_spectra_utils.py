from scripts.ingests.utils import (
    SimpleError,
    find_source_in_db,
    find_publication,
    check_internet_connection,
)

__all__ = [
    "ingest_spectra",
    "ingest_spectrum",
]


def ingest_spectra(
    db,
    sources,
    spectra,
    regimes,
    telescopes,
    instruments,
    modes,
    obs_dates,
    references,
    original_spectra=None,
    wavelength_units=None,
    flux_units=None,
    wavelength_order=None,
    comments=None,
    other_references=None,
    raise_error=True,
):
    """

    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
    sources: list[str]
        List of source names
    spectra: list[str]
        List of filenames corresponding to spectra files
    regimes: str or list[str]
        List or string
    telescopes: str or list[str]
        List or string
    instruments: str or list[str]
        List or string
    modes: str or list[str]
        List or string
    obs_dates: str or datetime
        List of strings or datetime objects
    references: list[str]
        List or string
    original_spectra: list[str]
        List of filenames corresponding to original spectra files
    wavelength_units: str or list[str] or Quantity, optional
        List or string
    flux_units: str or list[str] or Quantity, optional
        List or string
    wavelength_order: list[int], optional
    comments: list[str], optional
        List of strings
    other_references: list[str], optional
        List of strings
    raise_error: bool

    """

    # Convert single value input values to lists
    if isinstance(sources, str):
        sources = [sources]

    if isinstance(spectra, str):
        spectra = [spectra]

    input_values = [
        regimes,
        telescopes,
        instruments,
        modes,
        obs_dates,
        wavelength_order,
        wavelength_units,
        flux_units,
        references,
        comments,
        other_references,
    ]
    for i, input_value in enumerate(input_values):
        if isinstance(input_value, str):
            input_values[i] = [input_value] * len(sources)
        elif isinstance(input_value, type(None)):
            input_values[i] = [None] * len(sources)
    (
        regimes,
        telescopes,
        instruments,
        modes,
        obs_dates,
        wavelength_order,
        wavelength_units,
        flux_units,
        references,
        comments,
        other_references,
    ) = input_values

    n_spectra = len(spectra)
    n_skipped = 0
    n_dupes = 0
    n_missing_instrument = 0
    n_added = 0
    n_blank = 0

    msg = f"Trying to add {n_spectra} spectra"
    logger.info(msg)

    for i, source in enumerate(sources):
        # TODO: check that spectrum can be read by astrodbkit

        ingest_spectrum(
            db,
            source,
            spectra[i],
            regimes[i],
            telescopes[i],
            instruments[i],
            modes[i],
            obs_dates[i],
            references[i],
            original_spectrum=original_spectra[i],
            raise_error=raise_error,
        )

    msg = (
        f"SPECTRA ADDED: {n_added} \n"
        f" Spectra with blank obs_date: {n_blank} \n"
        f" Suspected duplicates skipped: {n_dupes}\n"
        f" Missing Telescope/Instrument/Mode: {n_missing_instrument} \n"
        f" Spectra skipped for unknown reason: {n_skipped} \n"
    )
    if n_spectra == 1:
        logger.info(f"Added {source} : \n" f"{row_data}")
    else:
        logger.info(msg)

    if n_added + n_dupes + n_blank + n_skipped + n_missing_instrument != n_spectra:
        msg = "Numbers don't add up: "
        logger.error(msg)
        raise SimpleError(msg)

    spec_count = (
        db.query(Spectra.regime, func.count(Spectra.regime))
        .group_by(Spectra.regime)
        .all()
    )

    spec_ref_count = (
        db.query(Spectra.reference, func.count(Spectra.reference))
        .group_by(Spectra.reference)
        .order_by(func.count(Spectra.reference).desc())
        .limit(20)
        .all()
    )

    telescope_spec_count = (
        db.query(Spectra.telescope, func.count(Spectra.telescope))
        .group_by(Spectra.telescope)
        .order_by(func.count(Spectra.telescope).desc())
        .limit(20)
        .all()
    )

    # logger.info(f'Spectra in the database: \n {spec_count} \n {spec_ref_count} \n {telescope_spec_count}')

    return


def ingest_spectrum(db, source, spectrum, regime, telescope, instrument, mode,
                    obs_date, reference, original_spectrum=None, 
                    wavelength_units, flux_units, comments = None, other_references=None, 
                    local_spectrum=None, raise_error=True):
    """ 
    Parameters
    ----------
    db: astrodbkit2.astrodb.Database
        Database object created by astrodbkit2
    source: str
        source name
    spectrum: str
        URL or path to spectrum file
    regime: str
        Regime of spectrum (optical, NIR, radio, etc.)
    telescope: str
        Telescope used to obtain spectrum
    """

    # Get source name as it appears in the database
    db_name = find_source_in_db(db, source)

    if len(db_name) != 1:
        msg = f"No unique source match for {source} in the database"
        raise SimpleError(msg)
    else:
        db_name = db_name[0]

    # Check if spectrum file is accessible
    # First check for internet
    internet = check_internet_connection()
    if internet:
        request_response = requests.head(spectrum)
        status_code = (
            request_response.status_code
        )  # The website is up if the status code is 200
        if status_code != 200:
            n_skipped += 1
            msg = (
                "The spectrum location does not appear to be valid: \n"
                f"spectrum: {spectrum} \n"
                f"status code: {status_code}"
            )
            logger.error(msg)
            if raise_error:
                raise SimpleError(msg)
        else:
            msg = f"The spectrum location appears up: {spectrum}"
            logger.debug(msg)
        if original_spectrum is not None:
            request_response1 = requests.head(original_spectrum)
            status_code1 = request_response1.status_code
            if status_code1 != 200:
                n_skipped += 1
                msg = (
                    "The spectrum location does not appear to be valid: \n"
                    f"spectrum: {original_spectrum} \n"
                    f"status code: {status_code1}"
                )
                logger.error(msg)
                if raise_error:
                    raise SimpleError(msg)
            else:
                msg = f"The spectrum location appears up: {original_spectrum}"
                logger.debug(msg)
    else:
        msg = "No internet connection. Internet is needed to check spectrum files."
        raise SimpleError(msg)

    # Find what spectra already exists in database for this source
    source_spec_data = (
        db.query(db.Spectra).filter(db.Spectra.c.source == db_name).table()
    )

    # SKIP if observation date is blank
    # TODO: try to populate obs date from meta data in spectrum file
    if ma.is_masked(obs_date) or obs_date == "":
        obs_date = None
        missing_obs_msg = (
            f"Skipping spectrum with missing observation date: {source} \n"
        )
        missing_row_spe = f"{source, obs_dates[i], references[i]} \n"
        logger.info(missing_obs_msg)
        logger.debug(missing_row_spe)
        n_blank += 1
        continue
    else:
        try:
            obs_date = pd.to_datetime(
                obs_dates[i]
            )  # TODO: Another method that doesn't require pandas?
        except ValueError:
            n_skipped += 1
            if raise_error:
                msg = f"{source}: Can't convert obs date to Date Time object: {obs_dates[i]}"
                logger.error(msg)
                raise SimpleError
        except dateutil.parser._parser.ParserError:
            n_skipped += 1
            if raise_error:
                msg = f"{source}: Can't convert obs date to Date Time object: {obs_dates[i]}"
                logger.error(msg)
                raise SimpleError
            else:
                msg = f"Skipping {source} Can't convert obs date to Date Time object: {obs_dates[i]}"
                logger.warning(msg)
            continue

    # TODO: make it possible to ingest units and order
    row_data = [
        {
            "source": db_name,
            "spectrum": spectra[i],
            "original_spectrum": None,  # if ma.is_masked(original_spectra[i]) or isinstance(original_spectra,None)
            # else original_spectra[i],
            "local_spectrum": None,  # if ma.is_masked(local_spectra[i]) else local_spectra[i],
            "regime": regimes[i],
            "telescope": telescopes[i],
            "instrument": None if ma.is_masked(instruments[i]) else instruments[i],
            "mode": None if ma.is_masked(modes[i]) else modes[i],
            "observation_date": obs_date,
            "wavelength_units": None
            if ma.is_masked(wavelength_units[i])
            else wavelength_units[i],
            "flux_units": None if ma.is_masked(flux_units[i]) else flux_units[i],
            "wavelength_order": None
            if ma.is_masked(wavelength_order[i])
            else wavelength_order[i],
            "comments": None if ma.is_masked(comments[i]) else comments[i],
            "reference": references[i],
            "other_references": None
            if ma.is_masked(other_references[i])
            else other_references[i],
        }
    ]
    logger.debug(row_data)

    try:
        with db.engine.connect() as conn:
            conn.execute(db.Spectra.insert().values(row_data))
            conn.commit()
        n_added += 1
    except sqlalchemy.exc.IntegrityError as e:
        if "CHECK constraint failed: regime" in str(e):
            msg = f"Regime provided is not in schema: {regimes[i]}"
            logger.error(msg)
            if raise_error:
                raise SimpleError(msg)
            else:
                continue
        if (
            db.query(db.Publications)
            .filter(db.Publications.c.publication == references[i])
            .count()
            == 0
        ):
            msg = (
                f"Spectrum for {source} could not be added to the database because the reference {references[i]} is not in Publications table. \n"
                f"(Add it with ingest_publication function.) \n "
            )
            logger.warning(msg)
            if raise_error:
                raise SimpleError(msg)
            else:
                continue
            # check telescope, instrument, mode exists
        telescope = (
            db.query(db.Telescopes)
            .filter(db.Telescopes.c.name == row_data[0]["telescope"])
            .table()
        )
        instrument = (
            db.query(db.Instruments)
            .filter(db.Instruments.c.name == row_data[0]["instrument"])
            .table()
        )
        mode = (
            db.query(db.Modes)
            .filter(db.Modes.c.name == row_data[0]["mode"])
            .table()
        )

        if len(source_spec_data) > 0:  # Spectra data already exists
            # check for duplicate measurement
            ref_dupe_ind = source_spec_data["reference"] == references[i]
            date_dupe_ind = source_spec_data["observation_date"] == obs_date
            instrument_dupe_ind = source_spec_data["instrument"] == instruments[i]
            mode_dupe_ind = source_spec_data["mode"] == modes[i]
            if (
                sum(ref_dupe_ind)
                and sum(date_dupe_ind)
                and sum(instrument_dupe_ind)
                and sum(mode_dupe_ind)
            ):
                msg = f"Skipping suspected duplicate measurement\n{source}\n"
                msg2 = f"{source_spec_data[ref_dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference']}"
                msg3 = f"{instruments[i], modes[i], obs_date, references[i], spectra[i]} \n"
                logger.warning(msg)
                logger.debug(msg2 + msg3 + str(e))
                n_dupes += 1
                if raise_error:
                    raise SimpleError
                else:
                    continue  # Skip duplicate measurement
            # else:
            #     msg = f'Spectrum could not be added to the database (other data exist): \n ' \
            #           f"{source, instruments[i], modes[i], obs_date, references[i], spectra[i]} \n"
            #     msg2 = f"Existing Data: \n "
            #            # f"{source_spec_data[ref_dupe_ind]['source', 'instrument', 'mode', 'observation_date', 'reference', 'spectrum']}"
            #     msg3 = f"Data not able to add: \n {row_data} \n "
            #     logger.warning(msg + msg2)
            #     source_spec_data[ref_dupe_ind][
            #               'source', 'instrument', 'mode', 'observation_date', 'reference', 'spectrum'].pprint_all()
            #     logger.debug(msg3)
            #     n_skipped += 1
            #     continue
        if len(instrument) == 0 or len(mode) == 0 or len(telescope) == 0:
            msg = (
                f"Spectrum for {source} could not be added to the database. \n"
                f" Telescope, Instrument, and/or Mode need to be added to the appropriate table. \n"
                f" Trying to find telescope: {row_data[0]['telescope']}, instrument: {row_data[0]['instrument']}, "
                f" mode: {row_data[0]['mode']} \n"
                f" Telescope: {telescope}, Instrument: {instrument}, Mode: {mode} \n"
            )
            logger.error(msg)
            n_missing_instrument += 1
            if raise_error:
                raise SimpleError
            else:
                continue
        else:
            msg = f"Spectrum for {source} could not be added to the database for unknown reason: \n {row_data} \n "
            logger.error(msg)
            raise SimpleError(msg)