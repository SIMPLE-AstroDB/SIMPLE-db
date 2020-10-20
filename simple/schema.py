# Schema for the SIMPLE database

from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String, Enum, DateTime, ForeignKeyConstraint
import enum
from astrodbkit2.astrodb import Base


# -------------------------------------------------------------------------------------------------------------------
# Reference tables
class Publications(Base):
    """ORM for publications table.
    This stores reference information (DOI, bibcodes, etc) and has shortname as the primary key
    """
    __tablename__ = 'Publications'
    name = Column(String(30), primary_key=True, nullable=False)
    bibcode = Column(String(100))
    doi = Column(String(100))
    description = Column(String(1000))


class Telescopes(Base):
    __tablename__ = 'Telescopes'
    name = Column(String(30), primary_key=True, nullable=False)
    reference = Column(String(30), ForeignKey('Publications.name', onupdate='cascade'))


class Instruments(Base):
    __tablename__ = 'Instruments'
    name = Column(String(30), primary_key=True, nullable=False)
    reference = Column(String(30), ForeignKey('Publications.name', onupdate='cascade'))


class Modes(Base):
    __tablename__ = 'Modes'
    name = Column(String(30), primary_key=True, nullable=False)
    instrument = Column(String(30), ForeignKey('Instruments.name', onupdate='cascade'), primary_key=True)
    telescope = Column(String(30), ForeignKey('Telescopes.name', onupdate='cascade'), primary_key=True)


# -------------------------------------------------------------------------------------------------------------------
# Hard-coded enumerations

class Regime(enum.Enum):
    """Enumeration for spectral type regime"""
    gammaray = 'gammaray'
    xray = 'xray'
    ultraviolet = 'ultraviolet'
    optical = 'optical'
    infrared = 'infrared'
    millimeter = 'millimeter'
    radio = 'radio'


class Gravity(enum.Enum):
    """Enumeration for gravity"""
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
    epoch = Column(String(10))  # eg, J2000
    equinox = Column(Float)  # decimal year
    shortname = Column(String(30))  # not needed?
    reference = Column(String(30), ForeignKey('Publications.name', onupdate='cascade'), nullable=False)
    comments = Column(String(1000))


class Names(Base):
    __tablename__ = 'Names'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    other_name = Column(String(100), primary_key=True, nullable=False)


class Photometry(Base):
    __tablename__ = 'Photometry'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    band = Column(String(30), primary_key=True)
    ucd = Column(String(100))
    magnitude = Column(Float, nullable=False)
    magnitude_error = Column(Float)
    # system = Column(String(30), ForeignKey('Systems.name'))
    telescope = Column(String(30), ForeignKey('Telescopes.name', onupdate='cascade'))
    instrument = Column(String(30), ForeignKey('Instruments.name', onupdate='cascade'))
    epoch = Column(String(30))
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', onupdate='cascade'), primary_key=True)


class Parallaxes(Base):
    # Table to store parallax values in milliarcseconds
    __tablename__ = 'Parallaxes'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    parallax = Column(Float, nullable=False)
    parallax_error = Column(Float)
    adopted = Column(Boolean)  # flag for indicating if this is the adopted measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', onupdate='cascade'), primary_key=True)


class ProperMotions(Base):
    # Table to store proper motions, in milliarcseconds per year
    __tablename__ = 'ProperMotions'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    mu_ra = Column(Float, nullable=False)
    mu_ra_error = Column(Float)
    mu_dec = Column(Float, nullable=False)
    mu_dec_error = Column(Float)
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), primary_key=True)


class RadialVelocities(Base):
    # Table to store radial velocities, in km/sec
    __tablename__ = 'RadialVelocities'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    radial_velocity = Column(Float, nullable=False)
    radial_velocity_error = Column(Float)
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), primary_key=True)


class SpectralTypes(Base):
    # Table to store spectral types, as strings
    __tablename__ = 'SpectralTypes'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    spectral_type = Column(String(10), nullable=False)
    spectral_type_error = Column(Float)
    regime = Column(Enum(Regime), primary_key=True)  # restricts to a few values: Optical, Infrared
    adopted = Column(Boolean)  # flag for indicating if this is the adopted measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), primary_key=True)


class Gravities(Base):
    # Table to store gravity measurements
    __tablename__ = 'Gravities'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)
    gravity = Column(Enum(Gravity), nullable=False)  # restricts to enumerated values
    regime = Column(Enum(Regime), primary_key=True)  # restricts to a few values: Optical, Infrared
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), primary_key=True)


class Spectra(Base):
    # Table to store references to spectra
    __tablename__ = 'Spectra'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'),
                    nullable=False, primary_key=True)

    # Data
    spectrum = Column(String(1000), nullable=False)  # URL of spectrum location
    local_spectrum = Column(String(1000))  # local directory (via environment variable) of spectrum location

    # Metadata
    regime = Column(Enum(Regime), primary_key=True)  # eg, Optical, Infrared, etc
    telescope = Column(String(30))
    instrument = Column(String(30))
    mode = Column(String(30))  # eg, Prism, Echelle, etc
    observation_date = Column(DateTime, primary_key=True)
    wavelength_units = Column(String(20))
    flux_units = Column(String(20))
    wavelength_order = Column(Integer)

    # Common metadata
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), primary_key=True)

    # Foreign key constraints for telescope, instrument, mode; all handled via reference to Modes table
    __table_args__ = (ForeignKeyConstraint([telescope, instrument, mode],
                                           [Modes.telescope, Modes.instrument, Modes.name],
                                           onupdate="cascade"),
                      {})
