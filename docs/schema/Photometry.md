# Photometry
Photometry for Sources


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main  |
| band | Photometry band for this measurement; links to PhotometryFilters table | string | 30 |  |   |
| :exclamation:**magnitude** | Magnitude value for this entry | double |  | mag |   |
| magnitude_error | Uncertainty of this magnitude value | double |  | mag |   |
| telescope | Telescope, mission, or survey name; links to Telescopes table | string | 30 |  |   |
| epoch | Decimal year | double |  | yr |   |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note  |
| :exclamation:**reference** | Publication reference; links to Publications table | string | 30 |  |   |

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
