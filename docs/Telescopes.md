# Telescopes

The Telescopes table contains names and references for telescopes referred to in other tables.
Entries must exist in the Telescopes table prior to inserting data that refer to it.
The *telescope* column is required to be unique.
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description                    | Unit  | Data Type    | Key Type  |
|------------|--------------------------------|---|--------------|---|
| *telescope | Unique name for the telescope  |   | String(30)   | primary   |
| description | Long name of telescope |   | String(1000) |    |
| reference  | Reference to the telescope     |   | String(30)   | foreign: Publications.name |
