# Parallaxes

The Parallaxes table contains parallax measurements in mas for sources listed in the Sources table. 
The combination of *source* and *reference* is expected to be unique.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source            | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| *parallax          | Parallax value | mas | Float  |   |
| parallax_error    | Parallax uncertainty | mas | Float  |   |
| adopted    | Flag indicating if this is the adopted measurement |  | Boolean  |   |
| comments  | Free form comments |   | String(1000) |   |
| *reference | Reference |   | String(30) | primary and foreign: Publications.name |
