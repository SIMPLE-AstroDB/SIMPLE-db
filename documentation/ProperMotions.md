# ProperMotions

The ProperMotions table contains proper motion measurements in mas/yr for sources listed in the Sources table. 
The combination of *source* and *reference* is expected to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *source        | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source   |
| mu_ra         | Proper motion in RA | mas/yr | Float  |   |
| mu_ra_error   | Uncertainty of proper motion in RA | mas/yr | Float  |   |
| mu_dec        | Proper motion in Dec | mas/yr | Float  |   |
| mu_dec_error  | Uncertainty of proper motion in Dec | mas/yr | Float  |   |
| adopted    | Flag indicating if this is the adopted measurement |  | Boolean  |   |
| comments      | Free form comments |   | String(1000) |   |
| *reference     | Reference |   | String(30) | primary and foreign: Publications.name |
