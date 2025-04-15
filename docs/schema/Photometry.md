# Photometry
Photometry for Sources
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| source | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main | False |
| band | Photometry band for this measurement; links to PhotometryFilters table | string | 30 |  |  | True |
| magnitude | Magnitude value for this entry | double |  | mag |  | False |
| magnitude_error | Uncertainty of this magnitude value | double |  | mag |  | True |
| telescope | Telescope, mission, or survey name; links to Telescopes table | string | 30 |  |  | True |
| epoch | Decimal year | double |  | yr |  | True |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note | True |
| reference | Publication reference; links to Publications table | string | 30 |  |  | False |

### Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Photometry | ['#Photometry.source', '#Photometry.band', '#Photometry.reference'] | Primary key for Photometry table |

### Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Photometry source to Sources table | ['#Photometry.source'] | ['#Sources.source'] |
| ForeignKey | Link Photometry band to PhotometryFilters table | ['#Photometry.band'] | ['#PhotometryFilters.band'] |
| ForeignKey | Link Photometry telescope to Telescopes table | ['#Photometry.telescope'] | ['#Telescopes.telescope'] |
| ForeignKey | Link Photometry reference to Publications table | ['#Photometry.reference'] | ['#Publications.reference'] |

