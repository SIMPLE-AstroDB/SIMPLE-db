# CompanionRelationships
The CompanionRelationships table contains companions to sources listed in the Sources table. The combination of *source* and *companion_name* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<ins>source</ins> | Unique identifier for the source; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:<ins>companion_name</ins> | External identifier for the companion object. Does not link to Sources table. | string | 50 |  | meta.id  |
| projected_separation_arcsec | Projected separation between the source and companion | double |  | arcsec | pos.angDistance  |
| projected_separation_error | Uncertainty of the projected separation | double |  | arcsec | stat.error;pos.angDistance  |
| :exclamation:relationship | Relationship of the source to the companion, e.g., "parent", "sibling", "child" | string | 100 |  |   |
| comments | Free form comments | string | 1000 |  | meta.note  |
| reference | Reference; links to Publications table | string | 30 |  | meta.ref  |
| other_companion_names | Additional names for the companion object, comma delimited. | string | 1000 |  | meta.id  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_CompanionRelationships | ['#CompanionRelationships.source', '#CompanionRelationships.companion_name'] | Primary key for CompanionRelationships table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link CompanionRelationships source to Sources table | ['#CompanionRelationships.source'] | ['#Sources.source'] |
| Link CompanionRelationships reference to Publications table | ['#CompanionRelationships.reference'] | ['#Publications.reference'] |
