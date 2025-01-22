"""
Schema for the SIMPLE database
"""

import enum
from datetime import datetime

import sqlalchemy as sa
from astrodbkit.astrodb import Base
from astrodbkit.views import view
from astropy.io.votable.ucd import check_ucd
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    String,
)
from sqlalchemy.orm import validates

# -------------------------------------------------------------------------------------------------------------------
# Reference tables
REFERENCE_TABLES = [
    "Publications",
    "Telescopes",
    "Instruments",
    "Modes",
    "PhotometryFilters",
    "Versions",
    "Parameters",
    "Regimes",
]


class Publications(Base):
    """ORM for publications table.
    This stores reference information (DOI, bibcodes, etc)
    and has shortname as the primary key
    """
    __tablename__ = 'Publications'
    reference = Column(String(30), primary_key=True, nullable=False)
    bibcode = Column(String(100))
    doi = Column(String(100))
    description = Column(String(1000))

    @validates("reference")
    def validate_reference(self, key, value):
        if value is None or len(value) > 30:
            raise ValueError(f"Provided reference is invalid; too long or None: {value}")
        return value


class Telescopes(Base):
    __tablename__ = 'Telescopes'
    telescope = Column(String(30), primary_key=True, nullable=False)
    description = Column(String(1000))
    reference = Column(
        String(30), ForeignKey("Publications.reference", onupdate="cascade")
    )


class Instruments(Base):
    __tablename__ = 'Instruments'
    instrument = Column(String(30), primary_key=True, nullable=False)
    mode = Column(String(30), primary_key=True)
    telescope = Column(
        String(30),
        ForeignKey("Telescopes.telescope", onupdate="cascade"),
        primary_key=True,
    )
    description = Column(String(1000))
    reference = Column(
        String(30), ForeignKey("Publications.reference", onupdate="cascade")
    )


class Parameters(Base):
    """Table for storing possible parameters for the ModeledParameters table"""
    __tablename__ = 'Parameters'
    parameter = Column(String(30), primary_key=True, nullable=False)
    description = Column(String(1000))


class PhotometryFilters(Base):
    """
    ORM for filter table.
    This stores relationships between filters and instruments, telescopes,
    as well as wavelength and width
    """
    __tablename__ = 'PhotometryFilters'
    band = Column(String(30), primary_key=True, nullable=False)
    # of the form instrument.filter (see SVO)
    ucd = Column(String(100))
    effective_wavelength = Column(Float, nullable=False)
    width = Column(Float)

    @validates("band")
    def validate_band(self, key, value):
        if "." not in value:
            raise ValueError("Band name must be of the form instrument.filter")
        return value

    @validates("ucd")
    def validate_ucd(self, key, value):
        ucd_string = "phot;" + value
        if check_ucd(ucd_string, check_controlled_vocabulary=True) is False:
            raise ValueError(f"UCD {value} not in controlled vocabulary")
        return value

    @validates("effective_wavelength")
    def validate_wavelength(self, key, value):
        if value is None or value < 0:
            raise ValueError(f"Invalid effective wavelength received: {value}")
        return value


class Versions(Base):
    """
    ORM for Versions table
    This stores the version numbers for the database
    """
    __tablename__ = 'Versions'
    version = Column(String(30), primary_key=True, nullable=False)
    start_date = Column(String(30))
    end_date = Column(String(30))
    description = Column(String(1000))


class Regimes(Base):
    """
    ORM for Regimes table
    Values used by Spectra and SpectralTypes tables
    """

    __tablename__ = "Regimes"
    regime = Column(String(30), primary_key=True, nullable=False)
    description = Column(String(1000))


# -------------------------------------------------------------------------------------------------------------------
# Hard-coded enumerations

class Gravity(enum.Enum):
    """Enumeration for gravity"""
    # TODO: Fix enumerations; the variable name is what's used throughout the database
    a = 'alpha'
    b = 'beta'
    g = 'gamma'
    d = 'delta'
    bg = 'beta/gamma'
    unknown = 'unknown'
    vlg = 'vl-g'
    intg = 'int-g'
    fldg = 'fld-g'


# -------------------------------------------------------------------------------------------------------------------
# Main tables
class Sources(Base):
    """ORM for the sources table. This stores the main identifiers
    for our objects along with ra and dec"""
    __tablename__ = 'Sources'
    source = Column(String(100), primary_key=True, nullable=False)
    ra = Column(Float)
    dec = Column(Float)
    epoch = Column(Float)  # decimal year
    equinox = Column(String(10))  # eg, J2000
    shortname = Column(String(30))  # not needed?
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        nullable=False,
    )
    other_references = Column(String(100))
    comments = Column(String(1000))

    @validates("ra")
    def validate_ra(self, key, value):
        if value > 360 or value < 0:
            raise ValueError("RA not in allowed range (0..360)")
        return value

    @validates("dec")
    def validate_dec(self, key, value):
        if value > 90 or value < -90:
            raise ValueError("Dec not in allowed range (-90..90)")
        return value


class Names(Base):
    __tablename__ = 'Names'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )
    other_name = Column(String(100), primary_key=True, nullable=False)


class Photometry(Base):
    # Table to store photometry information
    __tablename__ = 'Photometry'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )
    band = Column(String(30), ForeignKey("PhotometryFilters.band"), primary_key=True)
    magnitude = Column(Float, nullable=False)
    magnitude_error = Column(Float)
    telescope = Column(String(30), ForeignKey('Telescopes.telescope'))
    epoch = Column(Float)  # decimal year
    comments = Column(String(1000))
    regime = Column(String(30), ForeignKey("Regimes.regime"))
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        primary_key=True,
    )


class Parallaxes(Base):
    # Table to store parallax values in milliarcseconds
    __tablename__ = 'Parallaxes'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )
    parallax = Column(Float, nullable=False)
    parallax_error = Column(Float)
    adopted = Column(Boolean)  # flag for indicating if this is the adopted
    comments = Column(String(1000))
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        primary_key=True,
    )

    @validates("parallax")
    def validate_value(self, key, value):
        if value is None or value < 0:
            raise ValueError(f"{key} not allowed to be 0 or lower")
        return value


class ProperMotions(Base):
    # Table to store proper motions, in milliarcseconds per year
    __tablename__ = 'ProperMotions'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )
    mu_ra = Column(Float, nullable=False)
    mu_ra_error = Column(Float)
    mu_dec = Column(Float, nullable=False)
    mu_dec_error = Column(Float)
    adopted = Column(Boolean)
    comments = Column(String(1000))
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        primary_key=True,
    )


class RadialVelocities(Base):
    # Table to store radial velocities, in km/sec
    __tablename__ = 'RadialVelocities'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )
    radial_velocity_km_s = Column(Float, nullable=False)
    radial_velocity_error_km_s = Column(Float)
    adopted = Column(Boolean)
    comments = Column(String(1000))
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        primary_key=True,
    )


class SpectralTypes(Base):
    # Table to store spectral types, as strings
    __tablename__ = 'SpectralTypes'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )
    spectral_type_string = Column(String(20), nullable=False, primary_key=True)
    spectral_type_code = Column(Float, nullable=False, primary_key=True)
    spectral_type_error = Column(Float)
    regime = Column(
        String(30),
        ForeignKey("Regimes.regime", ondelete="cascade", onupdate="cascade"),
        primary_key=True,
    )
    adopted = Column(Boolean)
    photometric = Column(Boolean)
    comments = Column(String(1000))
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        primary_key=True,
    )

    @validates("source", "spectral_type_code", "regime", "reference")
    def validate_required(self, key, value):
        if value is None:
            raise ValueError(f"Value required for {key}")
        return value


class Gravities(Base):
    # Table to store gravity measurements
    __tablename__ = 'Gravities'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )
    gravity = Column(Enum(Gravity, create_constraint=True, native_enum=False),
                     nullable=False)  # restricts to enumerated values
    regime = Column(
        String(30),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        primary_key=True,
    )
    comments = Column(String(1000))
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        primary_key=True,
    )


class Spectra(Base):
    # Table to store references to spectra
    __tablename__ = 'Spectra'

    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )

    # Data
    access_url = Column(String(1000), nullable=False)  # URL of spectrum location

    # URL of original spectrum location, if applicable
    original_spectrum = Column(String(1000))  
    # local directory (via environment variable) of spectrum location
    local_spectrum = Column(String(1000))

    regime = Column(
        String(30),
        ForeignKey("Regimes.regime", ondelete="cascade", onupdate="cascade"),
        primary_key=True,
    )
    telescope = Column(String(30), nullable=False)
    instrument = Column(String(30), nullable=False)
    mode = Column(String(30), nullable=False)  # eg, Prism, Echelle, etc
    observation_date = Column(DateTime, primary_key=True)

    # Common metadata
    comments = Column(String(1000))
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        primary_key=True,
    )
    other_references = Column(String(100))

    # Composite Foreign key constraints for instrument and mode
    __table_args__ = (
        ForeignKeyConstraint(
            [telescope, instrument, mode],
            [Instruments.telescope, Instruments.instrument, Instruments.mode],
            onupdate="cascade",
        ),
        {},
    )

    @validates("access_url", "regime", "source", "telescope", "instrument", "mode")
    def validate_required(self, key, value):
        if value is None:
            raise ValueError(f"Value required for {key}")
        return value

    @validates("observation_date")
    def validate_date(self, key, value):
        if value is None:
            raise ValueError(f"Invalid date received: {value}")
        elif not isinstance(value, datetime):
            # Convert to datetime for storing in the database
            # Will throw error if unable to convert
            print("WARNING: Value will be converted to ISO format.")
            value = datetime.fromisoformat(value)
        return value


class ModeledParameters(Base):
    # Table to store derived/inferred paramaters from models
    __tablename__ = 'ModeledParameters'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )

    parameter = Column(
        String(30),
        ForeignKey("Parameters.parameter", onupdate="cascade"),
        primary_key=True,
    )
    value = Column(Float, nullable=False)
    value_error = Column(Float)
    unit = Column(String(20))
    comments = Column(String(1000))
    reference = Column(
        String(30),
        ForeignKey("Publications.reference", onupdate="cascade"),
        primary_key=True,
    )


class CompanionRelationships(Base):
    # Table to store information about companions
    __tablename__ = 'CompanionRelationships'
    source = Column(
        String(100),
        ForeignKey("Sources.source", ondelete="cascade", onupdate="cascade"),
        nullable=False,
        primary_key=True,
    )
    companion_name = Column(String(100), nullable=False, primary_key=True)
    projected_separation_arcsec = Column(Float)
    projected_separation_error = Column(Float)
    relationship = Column(String(100), nullable=False)
    # Relationship of source to companion.
    # Options: Child, Sibling, Parent, Unresolved Parent
    comments = Column(String(1000))
    reference = Column(
        String(30), ForeignKey("Publications.reference", onupdate="cascade")
    )
    other_companion_names = Column(String(10000))  # other names of the companions

# -------------------------------------------------------------------------------------------------------------------
# Views


ParallaxView = view(
    "ParallaxView",
    Base.metadata,
    sa.select(
        Parallaxes.source.label("source"),
        Parallaxes.parallax.label("parallax"),
        Parallaxes.parallax_error.label("parallax_error"),
        (1000.0 / Parallaxes.parallax).label("distance"),  # distance in parsecs
        Parallaxes.comments.label("comments"),
        Parallaxes.reference.label("reference"),
    )
    .select_from(Parallaxes)
    .where(sa.and_(Parallaxes.adopted == 1, Parallaxes.parallax > 0)),
)

PhotometryView = view(
    "PhotometryView",
    Base.metadata,
    sa.select(
        Photometry.source.label("source"),
        sa.func.avg(
            sa.case((Photometry.band == "2MASS.J", Photometry.magnitude))
        ).label("2MASS.J"),
        sa.func.avg(
            sa.case((Photometry.band == "2MASS.H", Photometry.magnitude))
        ).label("2MASS.H"),
        sa.func.avg(
            sa.case((Photometry.band == "2MASS.Ks", Photometry.magnitude))
        ).label("2MASS.Ks"),
        sa.func.avg(
            sa.case((Photometry.band == "WISE.W1", Photometry.magnitude))
        ).label("WISE.W1"),
        sa.func.avg(
            sa.case((Photometry.band == "WISE.W2", Photometry.magnitude))
        ).label("WISE.W2"),
        sa.func.avg(
            sa.case((Photometry.band == "WISE.W3", Photometry.magnitude))
        ).label("WISE.W3"),
        sa.func.avg(
            sa.case((Photometry.band == "WISE.W4", Photometry.magnitude))
        ).label("WISE.W4"),
        sa.func.avg(
            sa.case((Photometry.band == "IRAC.I1", Photometry.magnitude))
        ).label("IRAC.I1"),
        sa.func.avg(
            sa.case((Photometry.band == "IRAC.I2", Photometry.magnitude))
        ).label("IRAC.I2"),
        sa.func.avg(
            sa.case((Photometry.band == "IRAC.I3", Photometry.magnitude))
        ).label("IRAC.I3"),
        sa.func.avg(
            sa.case((Photometry.band == "IRAC.I4", Photometry.magnitude))
        ).label("IRAC.I4"),
    )
    .select_from(Photometry)
    .group_by(Photometry.source),
)
