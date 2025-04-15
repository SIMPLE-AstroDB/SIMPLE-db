# Parallaxes
Parallaxes for Sources
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main | False |
| parallax | Parallax value for this entry, in mas | double |  | mas | pos.parallax | True |
| parallax_error | Uncertainty of this parallax value | double |  | mas | stat.error;pos.parallax | True |
| adopted | Flag to indicate if this is the adopted entry | boolean |  |  |  | True |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note | True |
| :exclamation:**reference** | Publication reference; links to Publications table | string | 30 |  | meta.ref | False |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Parallaxes | ['#Parallaxes.source', '#Parallaxes.reference'] | Primary key for Parallaxes table |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link Parallaxes source to Sources table | ['#Parallaxes.source'] | ['#Sources.source'] |
| ForeignKey | Link Parallaxes reference to Publications table | ['#Parallaxes.reference'] | ['#Publications.reference'] |

