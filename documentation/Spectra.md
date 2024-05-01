# Spectra

The Spectra table contains spectra for sources listed in the Sources table.
Spectra are stored as strings representing the full URL of the spectrum location.
The combination of *source*, *spectrum*, *regime*, *observation_date*, and *reference* is expected to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type    | Key Type  |
|---|---|---|--------------|---|
| *source           | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *spectrum         | URL of spectrum location |   | String(1000) | primary |
| original_spectrum | URL of original spectrum location, if applicable |   | String(1000) |  |
| local_spectrum    | Local path of spectrum   |   | String(1000) |  |
| *regime           | Regime of the spectrum, eg Optical, Infrared, etc |  |  | foreign: Regimes.regime |
| *telescope        | Name of telescope |  | String(30)   | foreign: Telescopes.telescope |
| *instrument       | Name of instrument |  | String(30)   | foreign: Instruments.instrument |
| *mode             | Mode of spectrum  |  | String(30)   | foreign: Instruments.mode |
| *observation_date | Observation date  |  | DateTime     | primary |
| comments          | Free form comments |   | String(1000) |   |
| *reference        | Primary Reference |   | String(30)   | primary and foreign: Publications.reference |
| other_references  | Other References |   | String(100)  |   |

Relevant functions: `spectra.ingest_spectrum`, `spectra.spectrum_plottable`, `spectra.find_spectra`

If the spectrum provided has been modified from the author-provided one, 
a link to the original spectrum can be provided in the `original_spectrum` column.

The local_spectrum is meant to store the path to a local copy of the spectrum with an 
environment variable to define part of the path (so it can be shared among other users). 
For example: `$ASTRODB_SPECTRA/infrared/filename.fits`

# Notes
 - An accurate observation date is required for a spectrum to be ingested.
 - Data based on data from multiple observation dates has 'Multiple observation dates' 
   indicated in the *comments* field.
   One of the dates should be used for the *observation_date*.
 - Spectra for companions should be associated with individual sources and not grouped with the primary source.

# Check if spectra are plottable by the website
   ```Python
   from simple.utils.spectra import spectrum_plottable
   file = <path to file>
   spectrum_plottable(file, show_plot=True)
   ```
   
