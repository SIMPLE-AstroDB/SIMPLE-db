# Sources
The Sources table contains all objects in the database alongside their coordinates. This is considered the 'primary' table in the database, as each source is expected to be unique and is referred to by all other object tables. 


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Unique identifier for the source | string | 100 |  | meta.id;src;meta.main  |
| ra | Right Ascension of the source, ICRS recommended | double |  | deg | pos.eq.ra;meta.main  |
| dec | Declination of the source, ICRS recommended | double |  | deg | pos.eq.dec;meta.main  |
| epoch | Decimal year for coordinates (e.g., 2015.5). Not needed if using ICRS coordinates. | double |  | yr |   |
| equinox | Equinox reference frame year (e.g., J2000) | string | 10 |  |   |
| shortname | Short name for the source. TO BE DELETED. | string | 30 |  | meta.id;src  |
| :exclamation:**reference** | Discovery reference for the source; links to Publications table | string | 30 |  | meta.ref;meta.main  |
| other_references | Additional references, comma-separated. | string | 100 |  | meta.ref  |
| comments | Free form comments | string | 1000 |  | meta.note  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Sources_source | ['#Sources.source'] | Primary key for Sources table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link Source reference to Publications table | ['#Sources.reference'] | ['#Publications.reference'] |
## Checks
| Description | Expression |
| --- | --- |
| Validate RA range | ra >= 0 AND ra <= 360 |
| Validate Dec range | dec >= -90 AND dec <= 90 |
