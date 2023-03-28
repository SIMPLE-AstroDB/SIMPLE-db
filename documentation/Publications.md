# Publications

The Publications table contains metadata for each publication referred to in the database. 
Entries must exist in the Publications table prior to inserting data that refer to it.
The *name* of a publication is required to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|-------------|---|---|---|---|
| *reference  | Unique identifier for the publication |   | String(30)  | primary   |
| bibcode     | ADS Bibcode entry |   | String(100)  |    |
| doi         | Document Object Identifier (DOI) |   | String(100)  |    |
| description | Publication description |   | String(1000)  |    |

## Notes
-  `Reference` is typically a six letter string consisting of first four letters of first author name and two digit year. 
  - Smith et al. 2020 would be `Smit20`.
  - In the case of short last names, either first letters of first name or underscores can be used to construct the four letter string. 
    For example, `WuXi21` or `Wu__21`
  - In the case of multiple publications in the same year, a 2-4 letter string is appended which corresponds to the 
    last digits of the DOI or Bibcode. For example, `Smit20.123`. Avoid using `abc` suffixes.
    
- Relevant ingest functions:
  - `add_publication`
  - `search_publication`
  - `update_publication`