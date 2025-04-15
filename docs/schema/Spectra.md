# Spectra
Spectra for Sources
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| source | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main | False |
| access_url | URL for accessing the spectrum | string | 100 |  | meta.ref.url | True |
| original_spectrum | URL for the original spectrum | string | 1000 |  | meta.ref.url | True |
| local_spectrum | Local path (via environment variable) to the spectrum file | string | 1000 |  | meta.ref | True |
| regime | Spectral regime; links to Regimes table | string | 30 |  | meta.id | True |
| telescope | Telescope, mission, or survey name; links to reference in the Instruments table | string | 30 |  |  | True |
| instrument | Instrument name; links to Instruments table | string | 30 |  |  | True |
| mode | Instrument mode; links to Instruments table | string | 30 |  |  | True |
| observation_date | Date of the observation | timestamp |  |  | time.epoch | True |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note | True |
| reference | Publication reference; links to Publications table | string | 30 |  | meta.ref | False |
| other_references | Additional references | string | 100 |  | meta.ref | True |

### Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Spectra | ['#Spectra.source', '#Spectra.regime', '#Spectra.observation_date', '#Spectra.reference'] | Primary key for Spectra table |

### Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Spectra source to Sources table | ['#Spectra.source'] | ['#Sources.source'] |
| ForeignKey | Link Spectra regime to Regimes table | ['#Spectra.regime'] | ['#Regimes.regime'] |
| ForeignKey | Link Spectra telescope to Instruments table | ['#Spectra.telescope', '#Spectra.instrument', '#Spectra.mode'] | ['#Instruments.telescope', '#Instruments.instrument', '#Instruments.mode'] |
| ForeignKey | Link Spectra reference to Publications table | ['#Spectra.reference'] | ['#Publications.reference'] |

