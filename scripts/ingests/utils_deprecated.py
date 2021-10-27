# Make sure all source names are Simbad resolvable:
def check_names_simbad(ingest_names, ingest_ra, ingest_dec, radius='2s'):
    resolved_names = []
    n_sources = len(ingest_names)
    n_name_matches = 0
    n_selections = 0
    n_nearby = 0
    n_notfound = 0

    for i, ingest_name in enumerate(ingest_names):
        # Query Simbad for identifiers matching the ingest source name
        identifer_result_table = Simbad.query_object(ingest_name, verbose=False)

        # Successfully resolved one matching identifier in Simbad
        if identifer_result_table is not None and len(identifer_result_table) == 1:
            # Add the Simbad resolved identifier ot the resolved_name list and deals with unicode
            if isinstance(identifer_result_table['MAIN_ID'][0], str):
                resolved_names.append(identifer_result_table['MAIN_ID'][0])
            else:
                resolved_names.append(identifer_result_table['MAIN_ID'][0].decode())
            logger.debug(f"{resolved_names[i]}, Found name match in Simbad")
            n_name_matches = n_name_matches + 1

        # If no identifier match found, search within "radius" of coords for a Simbad object
        else:
            logger.debug(f"searching around, {ingest_name}")
            coord_result_table = Simbad.query_region(
                SkyCoord(ingest_ra[i], ingest_dec[i], unit=(u.deg, u.deg), frame='icrs'),
                radius=radius)

            # If no match is found in Simbad, use the name in the ingest table
            if coord_result_table is None:
                resolved_names.append(ingest_name)
                logger.debug("coord search failed")
                n_notfound = n_notfound + 1

            # If more than one match found within "radius", query user for selection and append to resolved_name
            elif len(coord_result_table) > 1:
                for j, name in enumerate(coord_result_table['MAIN_ID']):
                    logger.debug(f'{j}: {name}')
                selection = int(input('Choose \n'))
                if isinstance(coord_result_table['MAIN_ID'][selection], str):
                    resolved_names.append(coord_result_table['MAIN_ID'][selection])
                else:
                    resolved_names.append(coord_result_table['MAIN_ID'][selection].decode())
                logger.debug(f"{resolved_names[i]}, you selected")
                n_selections = n_selections + 1

            # If there is only one match found, accept it and append to the resolved_name list
            elif len(coord_result_table) == 1:
                if isinstance(coord_result_table['MAIN_ID'][0], str):
                    resolved_names.append(coord_result_table['MAIN_ID'][0])
                else:
                    resolved_names.append(coord_result_table['MAIN_ID'][0].decode())
                logger.debug(f"{resolved_names[i]}, only result nearby in Simbad")
                n_nearby = n_nearby + 1

    # Report how many find via which methods
    logger.info(f"Names Found:, {n_name_matches}")
    logger.info(f"Names Selected, {n_selections}")
    logger.info(f"Names Found, {n_nearby}")
    logger.info(f"Not found, {n_notfound}")

    n_found = n_notfound + n_name_matches + n_selections + n_nearby
    logger.warning('problem' if n_found != n_sources else (str(n_sources) + 'names'))

    return resolved_names