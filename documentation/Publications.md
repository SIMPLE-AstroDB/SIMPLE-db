# Publications

The Publications table contains metadata for each publication referred to in the database. 
Entries must exist in the Publications table prior to inserting data that refer to it.
The *name* of a publication is required to be unique.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| name          | Unique identifier for the publication |   | String(30)  | primary   |
| bibcode       | Bibcode entry |   | String(100)  |    |
| doi           | Document Object Identifier (DOI) |   | String(100)  |    |
| description   | Publication description, optional |   | String(1000)  |    |
