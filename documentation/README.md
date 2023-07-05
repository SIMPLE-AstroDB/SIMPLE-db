# SIMPLE Documentation

Here are the tables currently in our database, with links to their documentation. 
Object tables contain the main data for each unique source, while Reference tables contain information 
that is used across individual sources.

For data to be added to a new source, it must first exist in the Sources table. 
All sources must also contain at least one entry in the Names table, 
which can also list additional designations for the source.

Object Tables:
 - [Sources](Sources.md)
 - [Names](Names.md)
 - [Photometry](Photometry.md)
 - [Parallaxes](Parallaxes.md)
 - [ProperMotions](ProperMotions.md)
 - [RadialVelocities](RadialVelocities.md)
 - [SpectralTypes](SpectralTypes.md)
 - [Gravities](Gravities.md)
 - [Spectra](Spectra.md)
 - [ModeledParameters](ModeledParameters.md)
 
Reference Tables:
 - [Publications](Publications.md)
 - [Telescopes](Telescopes.md)
 - [Instruments](Instruments.md)
 - [PhometeryFilters](PhotometryFilters.md)
 - Versions

In addition to the physical tables, we also have database [Views](Views.md).