from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String, BigInteger, Enum, Date, DateTime
import enum
from simple.core import Base


# -------------------------------------------------------------------------------------------------------------------
# Reference tables
class Publications(Base):
    """ORM for publications table.
    This stores reference information (DOI, bibcodes, etc) and has shortname as the primary key
    """
    __tablename__ = 'Publications'
    # id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    shortname = Column(String(30), primary_key=True, nullable=False)
    bibcode = Column(String(100))
    doi = Column(String(100))
    description = Column(String(1000))


class Telescopes(Base):
    __tablename__ = 'Telescopes'
    name = Column(String(30), primary_key=True, nullable=False)
    reference = Column(String(30), ForeignKey('Publications.shortname'))


class Instruments(Base):
    __tablename__ = 'Instruments'
    name = Column(String(30), primary_key=True, nullable=False)
    reference = Column(String(30), ForeignKey('Publications.shortname'))


class Systems(Base):
    __tablename__ = 'Systems'
    name = Column(String(30), primary_key=True, nullable=False)


class Modes(Base):
    __tablename__ = 'Modes'
    name = Column(String(30), primary_key=True, nullable=False)


# -------------------------------------------------------------------------------------------------------------------
# Hard-coded enumerations
class Regime(enum.Enum):
    """Enumeration for spectral type regime"""
    optical = 'optical'
    infrared = 'infrared'
    ultraviolet = 'ultraviolet'
    radio = 'radio'


class Gravity(enum.Enum):
    """Enumeration for gravity
    TODO: BDNYC DB has a few extra ones. Need to convert/expand this list?
    """
    a = 'alpha'
    b = 'beta'
    g = 'gamma'
    bg = 'beta/gamma'
    unknown = 'unknown'


# -------------------------------------------------------------------------------------------------------------------
# Main tables
class Sources(Base):
    """ORM for the sources table. This stores the main identifiers for our objects along with ra and dec"""
    __tablename__ = 'Sources'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    ra = Column(Float)
    dec = Column(Float)
    designation = Column(String(100), unique=True, nullable=False)
    shortname = Column(String(30))
    reference = Column(String(30), ForeignKey('Publications.shortname'))
    comments = Column(String(1000))


class SpectralTypes(Base):
    __tablename__ = 'SpectralTypes'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    source_id = Column(Integer, ForeignKey('Sources.id'))
    spectral_type = Column(Float)
    spectral_type_error = Column(Float)
    gravity = Column(Enum(Gravity))  # restricts to enumerated values
    suffix = Column(String(30))
    regime = Column(Enum(Regime))  # restricts to two values: Optical, Infrared
    adopted = Column(Integer)
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.shortname'))
    gravity_reference = Column(String(30), ForeignKey('Publications.shortname'))


class Parallaxes(Base):
    __tablename__ = 'Parallaxes'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    source_id = Column(Integer, ForeignKey('Sources.id'))
    parallax = Column(Float)
    parallax_error = Column(Float)
    adopted = Column(Integer)
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.shortname'))


class Photometry(Base):
    __tablename__ = 'Photometry'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    source_id = Column(Integer, ForeignKey('Sources.id'))
    band = Column(String(30))
    magnitude = Column(Float)
    magnitude_error = Column(Float)
    system = Column(String(30), ForeignKey('Systems.name'))
    telescope = Column(String(30), ForeignKey('Telescopes.name'))
    instrument = Column(String(30), ForeignKey('Instruments.name'))
    epoch = Column(String(30))
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.shortname'))


class ProperMotions(Base):
    __tablename__ = 'ProperMotions'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    source_id = Column(Integer, ForeignKey('Sources.id'))
    mu_ra = Column(Float)
    mu_ra_error = Column(Float)
    mu_dec = Column(Float)
    mu_dec_error = Column(Float)
    vtan = Column(Float)
    vtan_error = Column(Float)
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.shortname'))


class Spectra(Base):
    __tablename__ = 'Spectra'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    source_id = Column(Integer, ForeignKey('Sources.id'))
    spectrum = Column(String(2000))  # URI/Path to file
    filename = Column(String(1000))  # filename
    wavelength_units = Column(String(30))
    flux_units = Column(String(30))
    order = Column(Integer)
    regime = Column(Enum(Regime))
    obs_date = Column(DateTime)
    telescope = Column(String(30), ForeignKey('Telescopes.name'))
    instrument = Column(String(30), ForeignKey('Instruments.name'))
    mode = Column(String(30), ForeignKey('Modes.name'))
    best = Column(Boolean)  # flag for indicating if this is the best spectrum or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.shortname'))


class RadialVelocities(Base):
    __tablename__ = 'RadialVelocities'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    source_id = Column(Integer, ForeignKey('Sources.id'))
    radial_velocity = Column(Float)
    radial_velocity_error = Column(Float)
    spectrum_id = Column(Integer, ForeignKey('Spectra.id'))
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.shortname'))


# -------------------------------------------------------------------------------------------------------------------
# TAP Schema tables
# TODO: Read over TAP schema documentation and see what's really needed
class TAPColumns(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'TAPColumns'
    table_name = Column(String(100))
    column_name = Column(String(100))
    description = Column(String(1000))
    unit = Column(String(100))
    ucd = Column(String(100))
    utype = Column(String(100))
    datatype = Column(String(100))
    size = Integer
    principal = Integer
    indexed = Integer
    std = Integer
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)  # added to avoid SQLAlchemy errors


class TAPKeyColumns(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'TAPKeyColumns'
    key_id = Column(String(100))
    from_column = Column(String(100))
    target_column = Column(String(100))
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)  # added to avoid SQLAlchemy errors


class TAPKeys(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'TAPKeys'
    key_id = Column(String(100))
    from_table = Column(String(100))
    target_table = Column(String(100))
    description = Column(String(1000))
    utype = Column(String(100))
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)  # added to avoid SQLAlchemy errors


class TAPSchemas(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'TAPSchemas'
    schema_name = Column(String(100))
    description = Column(String(1000))
    utype = Column(String(100))
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)  # added to avoid SQLAlchemy errors


class TAPTables(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'TAPTables'
    schema_name = Column(String(100))
    table_name = Column(String(100))
    table_type = Column(String(100))
    description = Column(String(1000))
    utype = Column(String(100))
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)  # added to avoid SQLAlchemy errors

