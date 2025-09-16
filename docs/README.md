# Demo Notebooks
- [Querying the Database](notebooks/Demo_Queries_1.0.ipynb)
- [Visualizing with the Database](notebooks/Demo_Visualization_1.0.ipynb)

# Schema Documentation
This documentation is generated from the [schema.yaml](simple/schema.yaml) file using [build_schema_docs.py](scripts/build_schema_docs.py).

## Tables
- [Publications](schema/Publications.md)
- [Telescopes](schema/Telescopes.md)
- [Instruments](schema/Instruments.md)
- [Parameters](schema/Parameters.md)
- [PhotometryFilters](schema/PhotometryFilters.md)
- [Versions](schema/Versions.md)
- [Regimes](schema/Regimes.md)
- [Sources](schema/Sources.md)
- [Names](schema/Names.md)
- [Photometry](schema/Photometry.md)
- [Parallaxes](schema/Parallaxes.md)
- [ProperMotions](schema/ProperMotions.md)
- [RadialVelocities](schema/RadialVelocities.md)
- [SpectralTypes](schema/SpectralTypes.md)
- [Gravities](schema/Gravities.md)
- [Spectra](schema/Spectra.md)
- [ModeledParameters](schema/ModeledParameters.md)
- [CompanionRelationships](schema/CompanionRelationships.md)
- [RotationalParameters](schema/RotationalParameters.md)

## Schema Diagram
This diagram is generated from the [schema.yaml](simple/schema.yaml) file using [make_erd.py](scripts/make_erd.py).
![Schema Diagram](figures/auto_schema.png)