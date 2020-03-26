# Adapted from https://stackoverflow.com/questions/51106264/how-do-i-split-an-sqlalchemy-declarative-model-into-modules
# And https://gist.github.com/bourque/6653dd69dadb3c1ee3d2ed6a9f3db2e5

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy import event
from sqlalchemy import create_engine

Base = declarative_base()


def load_connection(connection_string):
    """Return session, base, and engine objects for connecting to the database.

    Parameters
    ----------
    connection_string : str
        The connection string to connect to the database. The
        connection string should take the form:
        ``dialect+driver://username:password@host:port/database``

    Returns
    -------
    session : session object
        Provides a holding zone for all objects loaded or associated
        with the database.
    base : base object
        Provides a base class for declarative class definitions.
    engine : engine object
        Provides a source of database connectivity and behavior.
    """

    engine = create_engine(connection_string)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)
    session = Session()

    # Enable foreign key checks in SQLite
    if 'sqlite' in connection_string:
        set_sqlite()
    # elif 'postgresql' in connection_string:
    #     # Set up schema in postgres (must be lower case?)
    #     from sqlalchemy import DDL
    #     event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS ivoa"))
    #     event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS tap_schema"))

    return session, Base, engine


def set_sqlite():
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        # Enable foreign key checking in SQLite
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class Database:
    """
    Wrapper for database calls and utility functions
    """

    def __init__(self, connection_string):
        self.session, self.base, self.engine = load_connection(connection_string)

        # Convenience method
        self.query = self.session.query

        # Prep the tables
        self.metadata = self.base.metadata
        self.metadata.reflect(bind=self.engine)

        if len(self.metadata.tables) > 0:
            self._prepare_tables()
        else:
            print('Database empty. Import schema (eg, from simple.schema import *) and then run db.create_database()')

    def _prepare_tables(self):
        self.Sources = self.metadata.tables['Sources']
        self.Names = self.metadata.tables['Names']
        self.Publications = self.metadata.tables['Publications']
        self.SpectralTypes = self.metadata.tables['SpectralTypes']
        self.Gravities = self.metadata.tables['Gravities']
        self.Parallaxes = self.metadata.tables['Parallaxes']
        self.ProperMotions = self.metadata.tables['ProperMotions']
        self.ObsCore = self.metadata.tables['ObsCore']
        self.Photometry = self.metadata.tables['Photometry']
        self.RadialVelocities = self.metadata.tables['RadialVelocities']

    def create_database(self):
        # from simple.schema import *  # can't run from here?
        # self.base.metadata.drop_all()  # drop all the tables
        self.base.metadata.create_all()  # this explicitly create the SQLite file

        self._prepare_tables()

    def _inventory_query(self, data_dict, table, table_name, source_name):
        if table_name == 'ObsCore':
            results = self.session.query(table).filter(table.c.target_name == source_name).all()
        else:
            results = self.session.query(table).filter(table.c.source == source_name).all()

        if results and table_name == 'Sources':
            data_dict[table_name] = [row._asdict() for row in results]
        elif results:
            data_dict[table_name] = [self._row_cleanup(row) for row in results]

    @staticmethod
    def _row_cleanup(row):
        row_dict = row._asdict()
        del row_dict['source']
        return row_dict

    def inventory(self, name):
        data_dict = {}
        self._inventory_query(data_dict, self.Sources, 'Sources', name)
        self._inventory_query(data_dict, self.Names, 'Names', name)
        self._inventory_query(data_dict, self.Photometry, 'Photometry', name)
        self._inventory_query(data_dict, self.Parallaxes, 'Parallaxes', name)
        self._inventory_query(data_dict, self.ProperMotions, 'ProperMotions', name)
        self._inventory_query(data_dict, self.SpectralTypes, 'SpectralTypes', name)
        self._inventory_query(data_dict, self.Gravities, 'Gravities', name)
        self._inventory_query(data_dict, self.RadialVelocities, 'RadialVelocities', name)
        self._inventory_query(data_dict, self.ObsCore, 'ObsCore', name)

        return data_dict
