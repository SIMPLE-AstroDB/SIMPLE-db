# Sources
Main identifiers for objects along with coordinates..
 Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Unique identifier for an object | string | 100 |  | meta.id;src;meta.main  |
| ra | ICRS Right Ascension of object | double |  | deg | pos.eq.ra;meta.main  |
| dec | ICRS Declination of object | double |  | deg | pos.eq.dec;meta.main  |
| epoch | Decimal year for coordinates (eg, 2015.5) | double |  | yr |   |
| equinox | Equinox reference frame year (eg, J2000) | string | 10 |  |   |
| shortname | Short name for the source | string | 30 |  | meta.id;src  |
| :exclamation:**reference** | Publication reference; links to Publications table | string | 30 |  | meta.ref;meta.main  |
| other_references | Additional references | string | 100 |  | meta.ref  |
| comments | Free-form comments on this Source | string | 1000 |  | meta.note  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Sources_source | ['#Sources.source'] | Primary key for Sources table |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| Check | Validate RA range |  |  |
| Check | Validate Dec range |  |  |
| ForeignKey | Link Source reference to Publications table | ['#Sources.reference'] | ['#Publications.reference'] |

