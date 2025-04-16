# Photometry
The Photometry table contains photometric measurements for sources listed in the Sources table. The combination of *source*, *band*, and *reference* is expected to be unique. 


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<ins>source</ins> | Unique identifier for a source; links to Sources table | string | 100 |  | meta.id;meta.main  |
| band | Photometry band for this measurement; links to PhotometryFilters table | string | 30 |  |   |
| :exclamation:magnitude | Photometric magnitude | double |  | mag | phot.mag  |
| magnitude_error | Uncertainty of the magnitude | double |  | mag | stat.error;phot.mag  |
| telescope | Telescope, mission, or survey name; links to Telescopes table | string | 30 |  | instr.tel;instr.obsty  |
| epoch | Decimal year | double |  | yr | time.epoch  |
| comments | Free form comments | string | 1000 |  | meta.note  |
| :exclamation:<ins>reference</ins> | Reference; links to Publications table | string | 30 |  | meta.ref  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Photometry | ['#Photometry.source', '#Photometry.band', '#Photometry.reference'] | Primary key for Photometry table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link Photometry source to Sources table | ['#Photometry.source'] | ['#Sources.source'] |
| Link Photometry band to PhotometryFilters table | ['#Photometry.band'] | ['#PhotometryFilters.band'] |
| Link Photometry telescope to Telescopes table | ['#Photometry.telescope'] | ['#Telescopes.telescope'] |
| Link Photometry reference to Publications table | ['#Photometry.reference'] | ['#Publications.reference'] |
