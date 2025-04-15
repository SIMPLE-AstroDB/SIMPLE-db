# CompanionRelationships
Relationships between sources and their companions.
 Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 50 |  | meta.id;meta.main  |
| :exclamation:**companion_name** | External identifier for a companion object. Does not link to Sources table. | string | 50 |  | meta.id  |
| projected_separation_arcsec | Projected separation between the source and companion in arcseconds | double |  | arcsec | pos.angDistance  |
| projected_separation_error | Uncertainty of the projected separation in arcseconds | double |  | arcsec | stat.error;pos.angDistance  |
| :exclamation:**relationship** | Relationship of the source to the companion, e.g., "parent", "child", "sibling" | string | 100 |  |   |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note  |
| reference | Publication reference; links to Publications table | string | 30 |  | meta.ref  |
| other_companion_names | Additional names for the companion object, comma delimited. | string | 10000 |  | meta.id  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_CompanionRelationships | ['#CompanionRelationships.source', '#CompanionRelationships.companion_name'] | Primary key for CompanionRelationships table |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link CompanionRelationships source to Sources table | ['#CompanionRelationships.source'] | ['#Sources.source'] |
| ForeignKey | Link CompanionRelationships reference to Publications table | ['#CompanionRelationships.reference'] | ['#Publications.reference'] |

