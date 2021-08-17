# Telescopes

The Telescopes table contains names and references for telescopes referred to in other tables.
Entries must exist in the Telescopes table prior to inserting data that refer to it.
The *name* of a telescope is required to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *name      | Unique name for the telescope |   | String(30)  | primary   |
| *reference | Reference to the telescope |   | String(30) | foreign: Publications.name |
