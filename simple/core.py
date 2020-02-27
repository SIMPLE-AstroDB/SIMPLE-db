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

    return session, Base, engine


def set_sqlite():
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        # Enable foreign key checking in SQLite
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
