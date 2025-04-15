# Sources

The Sources table contains all objects in the database alongside their coordinates. 
This is considered the 'primary' table in the database, as each source 
is expected to be unique and is referred to by all other object tables. 
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source    | Unique identifier for the source |   | String(100)  | primary  |
| *ra        | Right Ascension | deg | Float  |   |
| *dec       | Declination | deg | Float  |   |
| epoch     | Decimal year for coordinates (eg, 2015.5) | year | Float |   |
| equinox   | Equinox reference frame year (eg, J2000) |  | String(10) |   |
| shortname | Optional short designation |   | String(30) |   |
| *reference | Reference to source |   | String(30) | foreign: Publications.name |
| other_references  | Other References |   | String(100)  |   |
| comments  | Free form comments |   | String(1000) |   |

## Notes
- Epoch is the date the source is expected to be at the given coordinate. 
  This date
  is most relevant for high proper option objects and
- In the case of multiple discovery references, for example independent discovery, choose one reference for the `reference` column and put the rest in the `other_references` column.