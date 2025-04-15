# Sources
Main identifiers for objects along with coordinates.
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| source | Unique identifier for an object | string | 100 |  | meta.id;src;meta.main | False |
| ra | ICRS Right Ascension of object | double |  | deg | pos.eq.ra;meta.main | True |
| dec | ICRS Declination of object | double |  | deg | pos.eq.dec;meta.main | True |
| epoch | Decimal year for coordinates (eg, 2015.5) | double |  | yr |  | True |
| equinox | Equinox reference frame year (eg, J2000) | string | 10 |  |  | True |
| shortname | Short name for the source | string | 30 |  | meta.id;src | True |
| reference | Publication reference; links to Publications table | string | 30 |  | meta.ref;meta.main | False |
| other_references | Additional references | string | 100 |  | meta.ref | True |
| comments | Free-form comments on this Source | string | 1000 |  | meta.note | True |

### Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Sources_source | ['#Sources.source'] | Primary key for Sources table |

### Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| Check | Validate RA range |  |  |
| Check | Validate Dec range |  |  |
| ForeignKey | Link Source reference to Publications table | ['#Sources.reference'] | ['#Publications.reference'] |

