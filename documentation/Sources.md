# Sources

The Sources table contains all objects in the database alongside their coordinates. 
This is considered the 'primary' table in the database, as each source 
is expected to be unique and is referred to by all other object tables.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| source    | Unique identifier for the source |   | String(100)  | primary  |
| ra        | Right Ascension | deg | Float  |   |
| dec       | Declination | deg | Float  |   |
| epoch     | Epoch for coordinates (eg, J2000) |   | String(10) |   |
| equinox   | Decimal year (eg, 2015.5) | year | Float |   |
| shortname | Optional short designation |   | String(30) |   |
| reference | Reference to source |   | String(30) | foreign: Publications.name |
| comments  | Free form comments |   | String(1000) |   |
