# Parallaxes
The Parallaxes table contains parallax measurements for sources listed in the Sources table. The combination of *source* and *reference* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<ins>source</ins> | Unique identifier for a source; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:parallax | Parallax measurement for the source | double |  | mas | pos.parallax  |
| parallax_error | Uncertainty of the parallax value | double |  | mas | stat.error;pos.parallax  |
| adopted | Flag to indicate if this is the adopted measurement | boolean |  |  |   |
| comments | Free form comments | string | 1000 |  | meta.note  |
| :exclamation:<ins>reference</ins> | Reference; links to Publications table | string | 30 |  | meta.ref  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Parallaxes | ['#Parallaxes.source', '#Parallaxes.reference'] | Primary key for Parallaxes table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link Parallaxes source to Sources table | ['#Parallaxes.source'] | ['#Sources.source'] |
| Link Parallaxes reference to Publications table | ['#Parallaxes.reference'] | ['#Publications.reference'] |
