# ModeledParameters

The ModeledParameters table contains a range of derived/inferred parameters for sources listed in the Sources table. 
The combination of *source*, *parameter*, and *reference* is expected to be unique.
Note that *parameter* is a reference to the values listed in the Parameters table. 
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source    | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *parameter | Name of parameter |  | String(30)  | primary and foreign: Parameters.parameter |
| *value      | Value for parameter | | Float    |     |
| comments  | Free form comments |   | String(1000) |   |
| *reference | Reference |   | String(30) | primary and foreign: Publications.name |
