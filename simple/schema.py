# Schema for the SIMPLE database

from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String, BigInteger, Enum, Date, DateTime
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


# -------------------------------------------------------------------------------------------------------------------
# Hard-coded enumerations


# -------------------------------------------------------------------------------------------------------------------
# Main tables
class Sources(Base):
    """ORM for the sources table. This stores the main identifiers for our objects along with ra and dec"""
    __tablename__ = 'Sources'
    source = Column(String(100), primary_key=True, nullable=False)
    ra = Column(Float)
    dec = Column(Float)
    shortname = Column(String(30))  # not needed?
    reference = Column(String(30), ForeignKey('Publications.name', onupdate='cascade'), nullable=False)
    comments = Column(String(1000))


class Names(Base):
    __tablename__ = 'Names'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'), nullable=False, primary_key=True)
    other_name = Column(String(100), primary_key=True, nullable=False)


class Photometry(Base):
    __tablename__ = 'Photometry'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'), nullable=False, primary_key=True)
    band = Column(String(30), primary_key=True)
    ucd = Column(String(100))
    magnitude = Column(Float)
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
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'), nullable=False, primary_key=True)
    parallax = Column(Float)
    parallax_error = Column(Float)
    best = Column(Boolean)  # flag for indicating if this is the best measurement or not
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', onupdate='cascade'), primary_key=True)


class ProperMotions(Base):
    # Table to store proper motions, in milliarcseconds per year
    __tablename__ = 'ProperMotions'
    source = Column(String(100), ForeignKey('Sources.source', ondelete='cascade', onupdate='cascade'), nullable=False, primary_key=True)
    mu_ra = Column(Float)
    mu_ra_error = Column(Float)
    mu_dec = Column(Float)
    mu_dec_error = Column(Float)
    comments = Column(String(1000))
    reference = Column(String(30), ForeignKey('Publications.name', ondelete='cascade'), primary_key=True)
