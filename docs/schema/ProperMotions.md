# ProperMotions
Proper motions for Sources
| Column Name | Description | Datatype | Length | Units  | UCD | Nullable |
| --- | --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main | False |
| mu_ra | Proper motion in RA*cos(Dec) in mas/yr | double |  | mas/yr | pos.pm;pos.eq.ra | True |
| mu_ra_error | Uncertainty of the proper motion in RA | double |  | mas/yr | stat.error;pos.pm;pos.eq.ra | True |
| mu_dec | Proper motion in Dec in mas/yr | double |  | mas/yr | pos.pm;pos.eq.dec | True |
| mu_dec_error | Uncertainty of the proper motion value in Dec | double |  | mas/yr | stat.error;pos.pm;pos.eq.dec | True |
| adopted | Flag to indicate if this is the adopted entry | boolean |  |  |  | True |
| comments | Free-form comments for this entry | string | 1000 |  | meta.note | True |
| reference | Publication reference; links to Publications table | string | 30 |  |  | True |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_ProperMotions | ['#ProperMotions.source', '#ProperMotions.reference'] | Primary key for Proper Motions table |

## Constraints
| Type | Description | Columns | Referenced Columns |
| --- | --- | --- | --- |
| ForeignKey | Link ProperMotions source to Sources table | ['#ProperMotions.source'] | ['#Sources.source'] |
| ForeignKey | Link ProperMotions reference to Publications table | ['#ProperMotions.reference'] | ['#Publications.reference'] |

