# Spectra
The Spectra table contains spectra for sources listed in the Sources table. Spectra are stored as strings representing the full URL of the spectrum location. The combination of *source*, *regime*, *observation_date*, and *reference* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<u>source</u> | Unique identifier for a source; links to Sources table | string | 100 |  | meta.id;meta.main  |
| access_url | URL for accessing the spectrum | string | 100 |  | meta.ref.url;meta.main  |
| original_spectrum | URL for the original spectrum | string | 1000 |  | meta.ref.url  |
| local_spectrum | Local path (via environment variable) to the spectrum file | string | 1000 |  |   |
| regime | Spectral regime of spectrum; links to Regimes table | string | 30 |  | meta.id  |
| telescope | Telescope, mission, or survey name; links to the Telescopes table | string | 30 |  | instr.tel;instr.obsty  |
| instrument | Instrument name; links to Instruments table | string | 30 |  | instr  |
| mode | Instrument mode; links to Instruments table | string | 30 |  |   |
| :exclamation:<u>observation_date</u> | Date of the observation | timestamp |  |  | time.epoch  |
| comments | Free form comments | string | 1000 |  | meta.note  |
| :exclamation:<u>reference</u> | Reference; links to Publications table | string | 30 |  | meta.ref;meta.main  |
| other_references | Additional references | string | 100 |  | meta.ref  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Spectra | ['#Spectra.source', '#Spectra.regime', '#Spectra.observation_date', '#Spectra.reference'] | Primary key for Spectra table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link Spectra source to Sources table | ['#Spectra.source'] | ['#Sources.source'] |
| Link Spectra regime to Regimes table | ['#Spectra.regime'] | ['#Regimes.regime'] |
| Link Spectra telescope to Instruments table | ['#Spectra.telescope', '#Spectra.instrument', '#Spectra.mode'] | ['#Instruments.telescope', '#Instruments.instrument', '#Instruments.mode'] |
| Link Spectra reference to Publications table | ['#Spectra.reference'] | ['#Publications.reference'] |
