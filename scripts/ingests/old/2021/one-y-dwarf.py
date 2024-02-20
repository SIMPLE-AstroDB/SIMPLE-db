from scripts.ingests.ingest_utils import *

# shortname,source,reference,SpT,spt_ref,ra,dec,epoch,J_MKO,J_err,H_MKO,H_err,MKO_ref,W1_mag,W1_err,W2_mag,W2_err,W3_mag,W3_err,W4_mag,W4_err,WISE_ref,ch1_mag,ch1_err,ch2_mag,ch2_err,Spizter_ref,plx_mas,plx_err,muRA_masyr,muRA_err,muDEC_masyr,muDEC_err,astrometry_ref
# 0302-5817,WISEA J030237.52-581740.3,Tinn18,Y0,Tinn18,45.656121,-58.294668,2014.0,-,-,-,-,-,19.262,nan,15.809,0.091,12.940,nan,9.323,nan,Cutr12,18.187,0.092,15.844,0.019,Kirk19,56.1,4.4,41.2,4.9,-76.4,5.2,Kirk19

db = load_simpledb('SIMPLE.db',recreatedb=True)

ingest_sources(db, ['WISEA J030237.52-581740.3'], [45.656121], [-58.294668], epochs=[2014.0], references=['Tinn18'])

code = convert_spt_string_to_code(['Y0'])

spectral_type_data = [{'source': 'WISEA J030237.52-581740.3',
                       'spectral_type_string': 'Y0',
                       'spectral_type_code': code[0],
                       'regime': 'nir',
                       'reference': 'Tinn18'}]

db.SpectralTypes.insert().execute(spectral_type_data)

band = 'IRAC.I1'
mag = 18.187
mag_unc = 0.092

ingest_photometry(db,  ['WISEA J030237.52-581740.3'], band, mag, mag_unc,'Kirk19',
                  ucds='em.IR.3-4um', telescope = 'Spitzer', instrument='IRAC' )
band = 'IRAC.I2'
mag = 15.844
mag_unc = 0.019

ingest_photometry(db,  ['WISEA J030237.52-581740.3'], band, mag, mag_unc,'Kirk19',
                  ucds='em.IR.3-4um', telescope = 'Spitzer', instrument='IRAC' )


plx = 56.1
plx_unc = 4.4
plx_ref = 'Kirk19'

ingest_parallaxes(db,  ['WISEA J030237.52-581740.3'], plx, plx_unc, plx_ref)


pm_ra = 41.2
pm_ra_unc = 4.9
pm_dec = -76.4
pm_dec_unc = 5.2
ref = 'Kirk19'
ingest_proper_motions(db, ['WISEA J030237.52-581740.3'], pm_ra, pm_ra_unc, pm_dec, pm_dec_unc, ref)

db.save_json('WISEA J030237.52-581740.3', 'data')
