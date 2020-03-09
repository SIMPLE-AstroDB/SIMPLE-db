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
    name = Column(String(30), primary_key=True, nullable=False)
    bibcode = Column(String(100))
    doi = Column(String(100))
    description = Column(String(1000))


class Telescopes(Base):
    __tablename__ = 'Telescopes'
    name = Column(String(30), primary_key=True, nullable=False)
    reference = Column(String(30), ForeignKey('Publications.name'))


class Instruments(Base):
    __tablename__ = 'Instruments'
    name = Column(String(30), primary_key=True, nullable=False)
    reference = Column(String(30), ForeignKey('Publications.name'))


class Systems(Base):
    __tablename__ = 'Systems'
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
    name = Column(String(100), primary_key=True, nullable=False)
    ra = Column(Float)
    dec = Column(Float)
    shortname = Column(String(30))  # not needed?
    reference = Column(String(30), ForeignKey('Publications.name'), nullable=False)
    comments = Column(String(1000))


class Names(Base):
    __tablename__ = 'Names'
    source = Column(String(100), ForeignKey('Sources.name'), nullable=False, primary_key=True)
    other_name = Column(String(100), primary_key=True, nullable=False)


class SpectralTypes(Base):
    __tablename__ = 'SpectralTypes'
    source = Column(String(100), ForeignKey('Sources.name'), nullable=False, primary_key=True)
    spectral_type = Column(Float)
    spectral_type_error = Column(Float)
    regime = Column(Enum(Regime), primary_key=True)  # restricts to a few values: Optical, Infrared
    best = Column(Boolean)  # flag for indicating if this is the best measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name'), primary_key=True)


class Gravities(Base):
    __tablename__ = 'Gravities'
    source = Column(String(100), ForeignKey('Sources.name'), nullable=False, primary_key=True)
    gravity = Column(Enum(Gravity))  # restricts to enumerated values
    regime = Column(Enum(Regime), primary_key=True)  # restricts to a few values: Optical, Infrared
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name'), primary_key=True)


class Parallaxes(Base):
    __tablename__ = 'Parallaxes'
    source = Column(String(100), ForeignKey('Sources.name'), nullable=False, primary_key=True)
    parallax = Column(Float)
    parallax_error = Column(Float)
    best = Column(Boolean)  # flag for indicating if this is the best measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name'), primary_key=True)


class Photometry(Base):
    __tablename__ = 'Photometry'
    source = Column(String(100), ForeignKey('Sources.name'), nullable=False, primary_key=True)
    band = Column(String(30), primary_key=True)
    magnitude = Column(Float)
    magnitude_error = Column(Float)
    system = Column(String(30), ForeignKey('Systems.name'))
    telescope = Column(String(30), ForeignKey('Telescopes.name'))
    instrument = Column(String(30), ForeignKey('Instruments.name'))
    epoch = Column(String(30))
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name'), primary_key=True)


class ProperMotions(Base):
    __tablename__ = 'ProperMotions'
    source = Column(String(100), ForeignKey('Sources.name'), nullable=False, primary_key=True)
    mu_ra = Column(Float)
    mu_ra_error = Column(Float)
    mu_dec = Column(Float)
    mu_dec_error = Column(Float)
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name'), primary_key=True)


class RadialVelocities(Base):
    __tablename__ = 'RadialVelocities'
    source = Column(String(100), ForeignKey('Sources.name'), nullable=False, primary_key=True)
    radial_velocity = Column(Float)
    radial_velocity_error = Column(Float)
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name'), primary_key=True)


class ObsCore(Base):
    __tablename__ = 'ObsCore'
    # __table_args__ = {'schema': 'ivoa'}  # ivoa schema; not the right way to define in SQLAlchemy? wont work on SQLite
    dataproduct_type = Column(String(100))  # image, spectra, timeseries
    calib_level = Column(Integer)  # enumeration 0,1,2,3,4
    obs_collection = Column(String(100))
    obs_id = Column(String(100), primary_key=True)
    obs_publisher_did = Column(String(100))  # dataset identifier given by the publisher
    access_url = Column(String(2000))
    access_format = Column(String(100))
    access_estsize = Column(Integer)  # estimated size in kbyte
    target_name = Column(String(100), ForeignKey('Sources.name'), nullable=False, primary_key=True)  # source
    s_ra = Column(Float)
    s_dec = Column(Float)
    s_fov = Column(Float)  # diameter in deg
    s_region = Column(String(1000))
    s_xel1 = Column(Integer)  # number of elements along the first spatial axis
    s_xel2 = Column(Integer)
    s_resolution = Column(Float)  # spatial resolution of data as FWHM
    t_min = Column(Float)  # in MJD
    t_max = Column(Float)  # in MJD
    t_exptime = Column(Float)  # in sec
    t_resolution = Column(Float)
    t_xel = Column(Integer)  # number of elements along the time axis
    em_min = Column(Float)
    em_max = Column(Float)
    em_res_power = Column(Float)
    em_xel = Column(Integer)  # number of elements along the spectral axis
    o_ucd = Column(String(100))  # UCD of observable (e.g. phot.flux.density, phot.count, etc.)
    pol_states = Column(String(100))
    pol_xel = Column(Integer)  # number of polarization samples
    facility_name = Column(String(30), ForeignKey('Telescopes.name'))
    instrument_name = Column(String(30), ForeignKey('Instruments.name'))

    # My additions to ObsCore
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name'))


# -------------------------------------------------------------------------------------------------------------------
# TAP Schema tables
# TODO: Read over TAP schema documentation and see what's really needed
class TAPColumns(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'columns'
    # __table_args__ = {'schema': 'TAP_SCHEMA'}
    table_name = Column(String(100), primary_key=True)
    column_name = Column(String(100), primary_key=True)
    description = Column(String(1000))
    unit = Column(String(100))
    ucd = Column(String(100))
    utype = Column(String(100))
    datatype = Column(String(100))
    size = Integer
    principal = Integer
    indexed = Integer
    std = Integer


class TAPKeyColumns(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'key_columns'
    # __table_args__ = {'schema': 'TAP_SCHEMA'}
    key_id = Column(String(100), primary_key=True)
    from_column = Column(String(100), primary_key=True)
    target_column = Column(String(100), primary_key=True)


class TAPKeys(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'keys'
    # __table_args__ = {'schema': 'TAP_SCHEMA'}
    key_id = Column(String(100), primary_key=True)
    from_table = Column(String(100), primary_key=True)
    target_table = Column(String(100), primary_key=True)
    description = Column(String(1000))
    utype = Column(String(100))


class TAPSchemas(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'schemas'
    # __table_args__ = {'schema': 'TAP_SCHEMA'}
    schema_name = Column(String(100), primary_key=True)
    description = Column(String(1000))
    utype = Column(String(100))


class TAPTables(Base):
    """ORM for the TAP SCHEMA"""
    __tablename__ = 'tables'
    # __table_args__ = {'schema': 'TAP_SCHEMA'}
    schema_name = Column(String(100), primary_key=True)
    table_name = Column(String(100), primary_key=True)
    table_type = Column(String(100))
    description = Column(String(1000))
    utype = Column(String(100))

