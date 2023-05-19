"""
Schema for the SIMPLE database
"""

# pylint: disable=line-too-long, missing-class-docstring, unused-import, invalid-name, singleton-comparison

import enum
import sqlalchemy as sa
from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String, \
    BigInteger, Enum, Date, DateTime, ForeignKeyConstraint
from astrodbkit2.astrodb import Base
from astrodbkit2.views import view


# -------------------------------------------------------------------------------------------------------------------
# Reference tables
class Publications(Base):
    """ORM for publications table.
    This stores reference information (DOI, bibcodes, etc) and has shortname as the primary key
    """
    __tablename__ = 'Publications'
    reference = Column(String(30), primary_key=True, nullable=False)
    bibcode = Column(String(100))
    doi = Column(String(100))
    description = Column(String(1000))


class Telescopes(Base):
    __tablename__ = 'Telescopes'
    telescope = Column(String(30), primary_key=True, nullable=False)
    description = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'))


class Instruments(Base):
    __tablename__ = 'Instruments'
    instrument = Column(String(30), primary_key=True, nullable=False)
    description = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'))


class Modes(Base):
    __tablename__ = 'Modes'
    mode = Column(String(30), primary_key=True, nullable=False)
    instrument = Column(String(30), ForeignKey('Instruments.instrument', onupdate='cascade'), primary_key=True)
    telescope = Column(String(30), ForeignKey('Telescopes.telescope', onupdate='cascade'), primary_key=True)
    description = Column(String(1000))


class Parameters(Base):
    """Table for storing possible parameters for the ModeledParameters table"""
    __tablename__ = 'Parameters'
    parameter = Column(String(30), primary_key=True, nullable=False)
    description = Column(String(1000))


class PhotometryFilters(Base):
    """
    ORM for filter table.
    This stores relationships between filters and instruments, telescopes, as well as wavelength and width
    """
    __tablename__ = 'PhotometryFilters'
    band = Column(String(30), primary_key=True, nullable=False)  # of the form instrument.filter (see SVO)
    instrument = Column(String(30), ForeignKey('Instruments.instrument', onupdate='cascade'), primary_key=True)
    telescope = Column(String(30), ForeignKey('Telescopes.telescope', onupdate='cascade'), primary_key=True)
    effective_wavelength = Column(Float, nullable=False)
    width = Column(Float)


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


# -------------------------------------------------------------------------------------------------------------------
# Hard-coded enumerations

class Regime(enum.Enum):
    """Enumeration for spectral type, spectra, and photometry regimes
    Use UCD controlled vocabulary: https://www.ivoa.net/documents/UCD1+/20200212/PEN-UCDlist-1.4-20200212.html#tth_sEcB
    The variable name is stored and used in the database; the string value should match it
    """
    ultraviolet = 'em.UV'
    optical_UCD = 'em.opt'
    optical = 'optical'
    nir_UCD = 'em.IR.NIR'  # Near-Infrared, 1-5 microns
    nir = 'nir'
    infrared = 'em.IR'  # Infrared part of the spectrum
    mir_UCD = 'em.IR.MIR'  # Medium-Infrared, 5-30 microns
    mir = 'mir'
    millimeter = 'em.mm'
    radio = 'em.radio'
    unknown = 'unknown'


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
    """ORM for the sources table. This stores the main identifiers for our objects along with ra and dec"""
    __tablename__ = 'Sources'
    source = Column(String(100), primary_key=True, nullable=False)
    ra = Column(Float)
    dec = Column(Float)
    epoch = Column(Float)  # decimal year
    equinox = Column(String(10))  # eg, J2000
    shortname = Column(String(30))  # not needed?
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), nullable=False)
    other_references = Column(String(100))
    comments = Column(String(1000))


class Names(Base):
    __tablename__ = 'Names'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    other_name = Column(String(100), primary_key=True, nullable=False)


class Photometry(Base):
    #TODO: Constrain UCD with Regime enumeration
    __tablename__ = 'Photometry'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    band = Column(String(30), primary_key=True)
    ucd = Column(String(100))
    magnitude = Column(Float, nullable=False)
    magnitude_error = Column(Float)
    telescope = Column(String(30))
    instrument = Column(String(30))
    epoch = Column(Float)  # decimal year
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), primary_key=True)

    # Foreign key constraints for telescope, instrument, band; all handled via reference to Modes table
    __table_args__ = (ForeignKeyConstraint([telescope, instrument, band],
                                           [PhotometryFilters.telescope, PhotometryFilters.instrument,
                                            PhotometryFilters.band],
                                           onupdate="cascade"),
                      {})


class Parallaxes(Base):
    # Table to store parallax values in milliarcseconds
    __tablename__ = 'Parallaxes'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    parallax = Column(Float, nullable=False)
    parallax_error = Column(Float)
    adopted = Column(Boolean)  # flag for indicating if this is the adopted measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), primary_key=True)


class ProperMotions(Base):
    # Table to store proper motions, in milliarcseconds per year
    __tablename__ = 'ProperMotions'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    mu_ra = Column(Float, nullable=False)
    mu_ra_error = Column(Float)
    mu_dec = Column(Float, nullable=False)
    mu_dec_error = Column(Float)
    adopted = Column(Boolean)  # flag for indicating if this is the adopted measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), primary_key=True)


class RadialVelocities(Base):
    # Table to store radial velocities, in km/sec
    __tablename__ = 'RadialVelocities'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    radial_velocity = Column(Float, nullable=False)
    radial_velocity_error = Column(Float)
    adopted = Column(Boolean)  # flag for indicating if this is the adopted measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), primary_key=True)


class SpectralTypes(Base):
    # Table to store spectral types, as strings
    __tablename__ = 'SpectralTypes'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    spectral_type_string = Column(String(10), nullable=False)
    spectral_type_code = Column(Float, nullable=False)
    spectral_type_error = Column(Float)
    regime = Column(Enum(Regime, create_constraint=True, native_enum=False),
                    primary_key=True)  # restricts to a few values: Optical, Infrared
    adopted = Column(Boolean)  # flag for indicating if this is the adopted measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), primary_key=True)


class Gravities(Base):
    # Table to store gravity measurements
    __tablename__ = 'Gravities'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    gravity = Column(Enum(Gravity, create_constraint=True, native_enum=False),
                     nullable=False)  # restricts to enumerated values
    regime = Column(Enum(Regime, create_constraint=True, native_enum=False),
                    primary_key=True)  # restricts to a few values: Optical, Infrared
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), primary_key=True)


class Spectra(Base):
    # Table to store references to spectra
    __tablename__ = 'Spectra'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)

    # Data
    spectrum = Column(String(1000), nullable=False)  # URL of spectrum location
    original_spectrum = Column(String(1000)) # URL of original spectrum location, if applicable
    local_spectrum = Column(String(1000))  # local directory (via environment variable) of spectrum location

    # Metadata
    regime = Column(Enum(Regime, create_constraint=True, values_callable=lambda x: [e.value for e in x],
                         native_enum=False),
                    primary_key=True)  # eg, Optical, Infrared, etc
    telescope = Column(String(30), ForeignKey(Telescopes.telescope))
    instrument = Column(String(30), ForeignKey(Instruments.instrument))
    mode = Column(String(30))  # eg, Prism, Echelle, etc
    observation_date = Column(DateTime, primary_key=True)

    # Common metadata
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), primary_key=True)
    other_references = Column(String(100))

    # Foreign key constraints for telescope, instrument, mode; all handled via reference to Modes table
    __table_args__ = (ForeignKeyConstraint([telescope, instrument, mode],
                                           [Modes.telescope, Modes.instrument, Modes.mode],
                                           onupdate="cascade"),
                      {})


class ModeledParameters(Base):
    # Table to store derived/inferred paramaters from models
    __tablename__ = 'ModeledParameters'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)

    parameter = Column(String(30), ForeignKey('Parameters.parameter', onupdate='cascade'), primary_key=True)
    value = Column(Float, nullable=False)
    value_error = Column(Float)
    unit = Column(String(20))
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.reference', onupdate='cascade'), primary_key=True)


# -------------------------------------------------------------------------------------------------------------------
# Views

ParallaxView = view(
    "ParallaxView",
    Base.metadata,
    sa.select(
        Parallaxes.source.label('source'),
        Parallaxes.parallax.label('parallax'),
        Parallaxes.parallax_error.label('parallax_error'),
        (1000./Parallaxes.parallax).label('distance'),  # distance in parsecs
        Parallaxes.comments.label('comments'),
        Parallaxes.reference.label('reference')
    ).select_from(Parallaxes)
    .where(sa.and_(Parallaxes.adopted == True, Parallaxes.parallax > 0)),
)

PhotometryView = view(
    "PhotometryView",
    Base.metadata,
    sa.select(
        Photometry.source.label('source'),
        sa.func.avg(sa.case((Photometry.band == "2MASS.J", Photometry.magnitude))).label("2MASS.J"),
        sa.func.avg(sa.case((Photometry.band == "2MASS.H", Photometry.magnitude))).label("2MASS.H"),
        sa.func.avg(sa.case((Photometry.band == "2MASS.Ks", Photometry.magnitude))).label("2MASS.Ks"),
        sa.func.avg(sa.case((Photometry.band == "WISE.W1", Photometry.magnitude))).label("WISE.W1"),
        sa.func.avg(sa.case((Photometry.band == "WISE.W2", Photometry.magnitude))).label("WISE.W2"),
        sa.func.avg(sa.case((Photometry.band == "WISE.W3", Photometry.magnitude))).label("WISE.W3"),
        sa.func.avg(sa.case((Photometry.band == "WISE.W4", Photometry.magnitude))).label("WISE.W4"),
        sa.func.avg(sa.case((Photometry.band == "IRAC.I1", Photometry.magnitude))).label("IRAC.I1"),
        sa.func.avg(sa.case((Photometry.band == "IRAC.I2", Photometry.magnitude))).label("IRAC.I2"),
        sa.func.avg(sa.case((Photometry.band == "IRAC.I3", Photometry.magnitude))).label("IRAC.I3"),
        sa.func.avg(sa.case((Photometry.band == "IRAC.I4", Photometry.magnitude))).label("IRAC.I4"),
    ).select_from(Photometry)
    .group_by(Photometry.source)
)
