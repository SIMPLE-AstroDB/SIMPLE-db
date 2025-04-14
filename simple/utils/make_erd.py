# Script to generate an Entity-Relation Diagram (ERD) for the database

import sys

import yaml
from eralchemy2 import render_er
from felis.datamodel import Schema
from felis.metadata import MetaDataBuilder

sys.path.append("./")  # needed for github actions to find the template module

# Load up schema
data = yaml.safe_load(open("simple/schema.yaml", "r"))
schema = Schema.model_validate(data)

# Create from Felis schema
metadata = MetaDataBuilder(schema).build()

# Create ER model from the database metadata
filename = "simple/schema.png"
render_er(metadata, filename)
