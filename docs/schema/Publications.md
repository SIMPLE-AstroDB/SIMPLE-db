# Publications
The Publications table contains metadata (DOI, bibcodes, etc) for each publication referred to in the database. The *reference* short identifer of each publication is required to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<u>reference</u> | Publication short identifier | string | 30 |  | meta.ref;meta.main  |
| bibcode | Publication bibcode from NASA ADS | string | 100 |  | meta.bib.bibcode  |
| doi | Publication Document Object Identifier (DOI) | string | 100 |  | meta.ref.doi  |
| description | Publication description | string | 1000 |  | meta.title  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Publications | ['#Publications.reference'] | Primary key for Publications table |

