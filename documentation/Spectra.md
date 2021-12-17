# Spectra

The Spectra table contains spectra for sources listed in the Sources table.
Spectra are stored as strings representing the full URL of the spectrum location.
The combination of *source*, *spectrum*, *regime*, *observation_date*, and *reference* is expected to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source           | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *spectrum          | URL of spectrum location |   | String(1000) | primary |
| local_spectrum    | Local path of spectrum   |   | String(1000) |  |
| *regime            | Regime of the spectrum, eg Optical, Infrared, etc |  | Enumeration | primary |
| *telescope         | Name of telescope |  | String(30)  | foreign: Telescopes.name |
| *instrument        | Name of instrument |  | String(30)  | foreign: Instruments.name |
| *mode              | Mode of spectrum  |  | String(30)  | foreign: Modes.name |
| *observation_date  | Observation date  |  | DateTime    | primary |
| wavelength_units  | Units for wavelength | | String(20) | |
| flux_units        | Units for flux   | | String(20) | |
| wavelength_order  | Wavelength order | | Integer | |
| comments          | Free form comments |   | String(1000) |   |
| *reference        | Primary Reference |   | String(30) | primary and foreign: Publications.name |
| *other_references  | Other References |   | String(30) |   |

The local_spectrum is meant to store the path to a local copy of the spectrum with an 
environment variable to define part of the path (so it can be shared among other users). 
For example: `$ASTRODB_SPECTRA/infrared/filename.fits`

Enumerations for regime should be [UCDs](https://www.ivoa.net/documents/UCD1+/20210616/EN-UCDlist-1.4-20210616.html#tth_sEc2). 
They currently  include:
 - em.UV
 - em.opt
 - optical (*deprecated, do not use*)
 - em.IR.NIR
 - nir (*deprecated, do not use*)
 - em.IR
 - em.IR.MIR 
 - mir (*deprecated, do not use*)
 - em.mm
 - em.radio 
 - unknown
 
# Notes
 - An accurate observation date is required for a spectrum to be ingested.
 - Data based on data from multiple observation dates has 'Multiple observation dates' 
   indicated in the *comments* field.
   One of the dates should be used for the *observation_date*.
 - Spectra for companions should be associated with individual sources and not grouped with the primary source.