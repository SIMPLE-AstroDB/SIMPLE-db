# Photometry Filters

The Photometry Filters table contains the names of the filters used by the Photometry table.
The combination of *source*, *band*, and *reference* is expected to be unique. 
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *band    | Band name | | String(30) | primary and foreign |
| *instrument | Name of Instrument  | | String(30) | primary and foreign |
| *telescope | Name of Telescope | | String(30) | primary and foreign |
| effective wavelength | Effective wavelength of band | Angstroms (10<sup>-10</sup>m) | |
| width | width of band | Angstroms (10<sup>-10</sup>m) | |

- Band names are listed at the [SVO filter profile service](http://svo2.cab.inta-csic.es/svo/theory/fps3/index.php?mode=browse&gname=Spitzer&asttype=).
