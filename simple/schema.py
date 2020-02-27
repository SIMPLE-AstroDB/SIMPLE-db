from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String
from simple.core import Base


class Publications(Base):
    """ORM for publications table.
    This stores reference information (DOI, bibcodes, etc) and has shortname as the primary key
    """

    __tablename__ = 'publications'
    # id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    shortname = Column(String(30), primary_key=True, nullable=False)
    bibcode = Column(String(100))
    doi = Column(String(100))
    description = Column(String(200))


class Sources(Base):
    """ORM for the sources table. This stores the main identifiers for our objects along with ra and dec"""

    __tablename__ = 'sources'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    ra = Column(Float)
    dec = Column(Float)
    designation = Column(String(100))
    shortname = Column(String(30))
    reference = Column(String(30), ForeignKey('publications.shortname'))

