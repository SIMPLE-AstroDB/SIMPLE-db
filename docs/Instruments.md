# Instruments

The Instruments table contains names and references for instruments referred to in other tables.
Entries must exist in the Instruments table prior to inserting data that refer to it.
The *instrument* column is required to be unique. 
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description                     | Unit  | Data Type | Key Type  |
|-------------|---------------------------------|---|---|---|
| *instrument | Unique name for the instrument  |   | String(30)  | primary   |
| description | Long name of instrument |   | String(1000) |    |
| reference   | Reference to the instrument |   | String(30) | foreign: Publications.name |
