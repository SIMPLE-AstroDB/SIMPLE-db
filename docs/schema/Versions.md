# Versions
Database version information


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<ins>version</ins> | Version identifier | string | 30 |  | meta.id;meta.main  |
| start_date | Date when this version started being used | string | 30 |  |   |
| end_date | Release date of this version | string | 30 |  |   |
| description | Description of changes associated with this version | string | 1000 |  |   |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Versions | ['#Versions.version'] | Primary key for Versions table |

