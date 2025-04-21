# Script to generate database from JSON contents
# This gets run automatically with Github Actions

import argparse
import sys
from astrodb_utils import load_astrodb

sys.path.append("./")
from simple import REFERENCE_TABLES

# Location of source data
DB_NAME = "SIMPLE.sqlite"
DB_PATH = "data/"
SCHEMA_PATH = "simple/schema.yaml"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate the SIMPLE database")
    args = parser.parse_args()

    # Run the loader for the specified DB architecture
    db = load_astrodb(
        DB_NAME,
        data_path=DB_PATH,
        recreatedb=True,
        reference_tables=REFERENCE_TABLES,
        felis_schema=SCHEMA_PATH,
    )
    print("New database generated.")

    # Close all connections
    db.session.close()
    db.engine.dispose()
