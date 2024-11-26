import logging
import sqlalchemy.exc
from astrodb_utils import (
    AstroDBError, find_source_in_db
)


__all__ = [
    "ingest_companion_relationships",
]

logger = logging.getLogger("SIMPLE")


def ingest_companion_relationships(
    db,
    source,
    companion_name,
    relationship,
    projected_separation_arcsec=None,
    projected_separation_error=None,
    comment=None,
    ref=None,
    other_companion_names=None,
):
    """
    This function ingests a single row in to the CompanionRelationship table

    Parameters
    ----------
    db: astrodbkit.astrodb.Database
        Database object created by astrodbkit
    source: str
        Name of source as it appears in sources table
    relationship: str
        relationship is of the souce to its companion
        should be one of the following: Child, Sibling, Parent, or Unresolved Parent
        see note
    companion_name: str
        SIMBAD resovable name of companion object
    projected_separation_arcsec: float (optional)
        Projected separtaion should be recorded in arc sec
    projected_separation_error: float (optional)
        Projected separtaion should be recorded in arc sec
    references: str (optional)
        Discovery references of sources
    comments: str (optional)
        Comments
    other_companion_names: comma separated names (optional)
        other names used to identify the companion
        ex:  'HD 89744, NLTT 24128, GJ 9326'

    Returns
    -------
    None

    Note: Relationships are constrained to one of the following:
    - *Child*: The source is lower mass/fainter than the companion
    - *Sibling*: The source is similar to the companion
    - *Parent*: The source is higher mass/brighter than the companion
    - *Unresolved Parent*: The source is the unresolved,
        combined light source of an unresolved
         multiple system which includes the companion

    """
    # checking relationship entered
    possible_relationships = ["Child", "Sibling", "Parent", "Unresolved Parent", None]
    # check captialization
    if relationship.title() != relationship:
        logger.info(
            f"Relationship captilization changed from "
            f"{relationship} to {relationship.title()} "
        )
        relationship = relationship.title()
    if relationship not in possible_relationships:
        msg = (
            f"Relationship given for {source}, {companion_name}: {relationship} "
            "NOT one of the constrained relationships \n {possible_relationships}"
        )
        logger.error(msg)
        raise AstroDBError(msg)

    # source canot be same as companion
    if source == companion_name:
        msg = f"{source}: Source cannot be the same as companion name"
        logger.error(msg)
        raise AstroDBError(msg)

    if source == companion_name:
        msg = f"{source}: Source cannot be the same as companion name"
        logger.error(msg)
        raise AstroDBError(msg)
    
    source_name = find_source_in_db(db, source)
    if len(source_name) != 1:
        msg = f"{source}: No source or multiple sources found: {source_name}"
        logger.error(msg)
        raise AstroDBError(msg)
    else:
        source_name = source_name[0]

    if projected_separation_arcsec is not None and projected_separation_arcsec < 0:
        msg = f"Projected separation: {projected_separation_arcsec}, cannot be negative"
        logger.error(msg)
        raise AstroDBError(msg)
    if projected_separation_error is not None and projected_separation_error < 0:
        msg = (
            f"Projected separation error: {projected_separation_error},"
            " cannot be negative"
        )
        logger.error(msg)
        raise AstroDBError(msg)

    # check other names
    # make sure companion name is included in the list
    if other_companion_names is None:
        other_companion_names = companion_name
    else:
        companion_name_list = other_companion_names.split(", ")
        if companion_name not in companion_name_list:
            companion_name_list.append(companion_name)
        other_companion_names = (",  ").join(companion_name_list)

    try:
        with db.engine.connect() as conn:
            conn.execute(
                db.CompanionRelationships.insert().values(
                    {
                        "source": source_name,
                        "companion_name": companion_name,
                        "projected_separation_arcsec": projected_separation_arcsec,
                        "projected_separation_error": projected_separation_error,
                        "relationship": relationship,
                        "reference": ref,
                        "comments": comment,
                        "other_companion_names": other_companion_names,
                    }
                )
            )
            conn.commit()
        logger.info(
            "ComapnionRelationship added: ",
            [
                source_name,
                companion_name,
                relationship,
                projected_separation_arcsec,
                projected_separation_error,
                comment,
                ref,
            ],
        )
    except sqlalchemy.exc.IntegrityError as e:
        if "UNIQUE constraint failed:" in str(e):
            msg = "The companion may be a duplicate."
            logger.error(msg)
            raise AstroDBError(msg)

        else:
            msg = (
                "Make sure all required parameters are provided. \\"
                "Other possible errors: source may not exist in Sources table \\"
                "or the reference may not exist in the Publications table. "
            )
            logger.error(msg)
            raise AstroDBError(msg)
