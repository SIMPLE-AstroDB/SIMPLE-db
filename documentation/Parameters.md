# Parameters

The Parameters table contains names and descriptions for parameters referred 
to in the ModeledParameters table.
Entries must exist in the Parameters table prior to inserting data that refer to it.
The *parameter* is required to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *parameter      | Short name for the parameter (eg, C/O ratio) |   | String(30)  | primary   |
| *description | Description of what this parameter is |   | String(1000) |  |
