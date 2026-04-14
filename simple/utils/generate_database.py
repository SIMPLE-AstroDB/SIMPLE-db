# Script to generate database from JSON contents
# This gets run automatically with Github Actions

import argparse
from astrodb_utils.loaders import build_db_from_json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate the SIMPLE database")
    args = parser.parse_args()

    db = build_db_from_json(settings_file="database.toml")

    print("New database generated.")

    # Close all connections
    db.session.close()
    db.engine.dispose()
