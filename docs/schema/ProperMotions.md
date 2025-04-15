# ProperMotions
The ProperMotions table contains proper motion measurements for sources listed in the Sources table. The combination of *source* and *reference* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:**source** | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:**mu_ra** | Proper motion in RA*cos(Dec) in mas/yr | double |  | mas/yr | pos.pm;pos.eq.ra  |
| mu_ra_error | Uncertainty of the proper motion in RA | double |  | mas/yr | stat.error;pos.pm;pos.eq.ra  |
| :exclamation:**mu_dec** | Proper motion in Dec in mas/yr | double |  | mas/yr | pos.pm;pos.eq.dec  |
| mu_dec_error | Uncertainty of the proper motion value in Dec | double |  | mas/yr | stat.error;pos.pm;pos.eq.dec  |
| adopted | Flag to indicate if this is the adopted entry | boolean |  |  |   |
| comments | Free form comments | string | 1000 |  | meta.note  |
| :exclamation:**reference** | Reference; links to Publications table | string | 30 |  |   |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_ProperMotions | ['#ProperMotions.source', '#ProperMotions.reference'] | Primary key for Proper Motions table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link ProperMotions source to Sources table | ['#ProperMotions.source'] | ['#Sources.source'] |
| Link ProperMotions reference to Publications table | ['#ProperMotions.reference'] | ['#Publications.reference'] |
