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
| *reference        | Reference |   | String(30) | primary and foreign: Publications.name |

The local_spectrum is meant to store the path to a local copy of the spectrum with an 
environment variable to define part of the path (so it can be shared among other users). 
For example: `$ASTRODB_SPECTRA/infrared/filename.fits`

Enumerations for regime include:
 - gammaray
 - xray
 - ultraviolet
 - optical
 - nir
 - infrared
 - millimeter
 - radio
 - unknown
 