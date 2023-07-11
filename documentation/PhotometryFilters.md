# Photometry Filters

The Photometry Filters table contains the names of the filters used by the Photometry table.
The combination of *source*, *band*, and *reference* is expected to be unique. 
Columns marked with an asterisk (*) may not be empty.

| Column Name | Description  | Unit  | Data Type | Key Type  |
|---|---|---|---|---|
| *band    | Band name | | String(30) | primary |
| ucd       | Unified Content Descriptor for band |  | String(100)  |   |
| effective wavelength | Effective wavelength of band | Angstroms (10<sup>-10</sup>m) | |
| width | width of band | Angstroms (10<sup>-10</sup>m) | |

## Notes
- Band names are listed at the [SVO filter profile service](http://svo2.cab.inta-csic.es/svo/theory/fps3/index.php?mode=browse&gname=Spitzer&asttype=).
- UCDs are listed in the [IVOA controlled vocabulary](https://www.ivoa.net/documents/UCD1+/20200212/PEN-UCDlist-1.4-20200212.html#tth_sEcB).
  Common ones for cool stars are:
  - `em.opt.R`	Optical band between 600 and 750 nm
  - `em.opt.I`	Optical band between 750 and 1000 nm
  - `em.IR.J`	Infrared between 1.0 and 1.5 micron
  - `em.IR.H`	Infrared between 1.5 and 2 micron 
  - `em.IR.K`	Infrared between 2 and 3 micron 
  - `em.IR.3-4um`	Infrared between 3 and 4 micron
  - `em.IR.4-8um`	Infrared between 4 and 8 micron
