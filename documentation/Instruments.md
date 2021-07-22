# Instruments

The Instruments table contains names and references for instruments referred to in other tables.
Entries must exist in the Instruments table prior to inserting data that refer to it.
The *name* of an instrument is required to be unique.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *name      | Unique name for the instrument |   | String(30)  | primary   |
| reference | Reference to the instrument |   | String(30) | foreign: Publications.name |
