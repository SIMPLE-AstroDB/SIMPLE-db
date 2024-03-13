# Script to generate database from JSON contents
# This gets run automatically with Github Actions

import argparse
import sys
import os
from astrodb_utils import load_astrodb

sys.path.append("./")
from simple.schema import *
from simple.schema import REFERENCE_TABLES

# Location of source data
DB_PATH = "data"
DB_NAME = "SIMPLE.sqlite"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate the SIMPLE database")
    # parser.add_argument(
    #    "architecture",
    #    choices=["sqlite", "postgres"],
    #    help="Database architecture to use.",
    # )
    # parser.add_argument(
    #    "connection_string",
    #    nargs="?",
    #    help="Connection string to use for non-sqlite databases.",
    # )

    args = parser.parse_args()

    # Get the connection string for any non-sqlite database
    # if args.connection_string is not None:
    #    connection_string = args.connection_string
    # else:
    connection_string = os.getenv("SIMPLE_DATABASE_URL", default="")

    # Run the loader for the specified DB architecture
    db = load_astrodb(DB_NAME, reference_tables=REFERENCE_TABLES)
    print("New database generated.")

    # Close all connections
    db.session.close()
    db.engine.dispose()
