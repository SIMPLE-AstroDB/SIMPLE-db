# Spectra
Spectra for Sources


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main  |
| access_url | URL for accessing the spectrum | string | 100 |  | meta.ref.url  |
| original_spectrum | URL for the original spectrum | string | 1000 |  | meta.ref.url  |
| local_spectrum | Local path (via environment variable) to the spectrum file | string | 1000 |  | meta.ref  |
| regime | Spectral regime; links to Regimes table | string | 30 |  | meta.id  |
| telescope | Telescope, mission, or survey name; links to reference in the Instruments table | string | 30 |  |   |
| instrument | Instrument name; links to Instruments table | string | 30 |  |   |
| mode | Instrument mode; links to Instruments table | string | 30 |  |   |
| :exclamation:**observation_date** | Date of the observation | timestamp |  |  | time.epoch  |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note  |
| :exclamation:**reference** | Publication reference; links to Publications table | string | 30 |  | meta.ref  |
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
