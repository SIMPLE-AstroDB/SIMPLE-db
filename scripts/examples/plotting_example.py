# Script to create some example plots for presentations
import logging
import numpy as np
import matplotlib.pyplot as plt
import astropy.units as u
from sqlalchemy import func, and_, Integer, cast
from scripts.ingests.utils import load_simpledb, logger
from scripts.ingests.ingest_utils import convert_spt_code_to_string_to_code
from astrodbkit2.spectra import load_spectrum

plt.interactive(False)
logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=False)

# ===============================================================================================
# Counts of spectral types

# Query for counts grouped by spectral type
t = db.query(cast(db.SpectralTypes.c.spectral_type_code, Integer()).label('spectral_type'),
             func.count(db.SpectralTypes.c.source).label('counts')). \
        group_by(cast(db.SpectralTypes.c.spectral_type_code, Integer())). \
        having(func.count(cast(db.SpectralTypes.c.spectral_type_code, Integer())) > 0). \
        astropy()
# Making strings out of the numeric codes
t['spectral_type'] = convert_spt_code_to_string_to_code(t['spectral_type'], decimals=0)

# Bar chart of counts vs spectral types
fig, ax = plt.subplots(figsize=(8, 6))
index = np.arange(len(t))
bar_width = 0.95
plt.bar(index, t['counts'], bar_width, alpha=0.8)
plt.xlabel('Spectral Type')
plt.ylabel('Counts')
plt.xticks(index, t['spectral_type'])
# plt.yscale('log')
ax.set_xticklabels(ax.xaxis.get_majorticklabels(), rotation=90)
plt.tight_layout()
# plt.show()
plt.savefig('plots/sptypes_counts.png')

# ===============================================================================================
# Representative spectra (1 per spectral type)

# Query for sources and spectral types that have SpeX Prism spectra

t = db.query(db.SpectralTypes.c.source, db.SpectralTypes.c.spectral_type_string,
             db.SpectralTypes.c.spectral_type_code,
             db.Spectra.c.instrument, db.Spectra.c.spectrum). \
    join(db.Spectra, db.Spectra.c.source == db.SpectralTypes.c.source). \
    filter(db.Spectra.c.instrument == 'SpeX'). \
    filter(db.Spectra.c.spectrum.ilike('%.fits')). \
    order_by(db.SpectralTypes.c.spectral_type_code). \
    astropy()

spectra_dict = {}
spectra_dict['spt'] = []
for row in t:
    # Only consider those from spectral types that haven't been fetched yet
    spt = convert_spt_code_to_string_to_code(row['spectral_type_code'], decimals=1)[0]
    if spt in spectra_dict.get('spt') or not spt.endswith('.0'):
        continue

    spec = load_spectrum(row['spectrum'], spectra_format='Spex Prism')
    if isinstance(spec, str):
        # Failed to get spectrum
        continue

    # Store results
    spectra_dict['spt'].append(spt)
    spectra_dict[f'{spt}_data'] = [row['source'], spt, spec]

# Make plot
fig, ax = plt.subplots(figsize=(8, 6))
ind = 0  # index counter for offsets
scale = 0.5  # how much to offset each spectrum
minwave, maxwave = 1.2, 1.3  # normalisation region bounds
# minwave, maxwave = 2.1, 2.25
for k, v in spectra_dict.items():
    if k == 'spt':
        continue

    print(v[0], v[1])

    # Normalize spectra and offset
    wave, flux = v[2].wavelength.to(u.micron).value, v[2].flux.value
    fluxreg = flux[(wave >= minwave) & (wave <= maxwave)]  # cut flux to region
    if len(fluxreg):
        fluxmed = np.nanmedian(fluxreg)
    else:
        fluxmed = np.nanmedian(flux)

    # Add to plot
    plt.plot(wave, flux / fluxmed + ind * scale, alpha=0.8, label=v[1], linewidth=2)
    ind += 1

plt.legend()
plt.xlabel('Wavelength')
plt.ylabel('Flux')
plt.tight_layout()
# plt.show()
plt.savefig('plots/spectra_sample.png')
