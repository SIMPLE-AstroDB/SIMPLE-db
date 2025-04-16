# Names
The Names table contains possible designations for sources in the Sources table. Every source must have an entry in the Names table. This table is not meant to be a comprehensive list of all designations for a source, but rather for storing unofficial names and for quick name matching independent of external services. The combination of *source* and *other_name* is expected to be unique.


Columns marked with an exclamation mark ( :exclamation:) may not be empty.
| Column Name | Description | Datatype | Length | Units  | UCD |
| --- | --- | --- | --- | --- | --- |
| :exclamation:<u>source</u> | Main identifier for an object; links to Sources table | string | 100 |  | meta.id;meta.main  |
| :exclamation:<u>other_name</u> | Alternate identifier for an object | string | 100 |  | meta.id  |

## Indexes
| Name | Columns | Description |
| --- | --- | --- |
| PK_Names_source | ['#Names.source', '#Names.other_name'] | Primary key for Names table |

## Foreign Keys
| Description | Columns | Referenced Columns |
| --- | --- | --- |
| Link Names primary identifer to Sources table | ['#Names.source'] | ['#Sources.source'] |
