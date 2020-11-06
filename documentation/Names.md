# Names

The Names table contains all known designations for a source in the Sources table. 
It is meant for quick name matching independent of external services. 
The combination of *source* and *other_name* is meant to be unique and every 
source should have an entry in the Names table.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| source        | Unique identifier for the source |   | String(100)  | primary and foreign: Sources.source  |
| other_name    | Alternative identifier for the source |   | String(100)  | primary  |