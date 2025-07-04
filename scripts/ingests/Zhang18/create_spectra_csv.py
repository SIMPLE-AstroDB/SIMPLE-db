from astropy.io import fits
from astropy.time import Time
import os
import csv


path = "/Users/guanying/SIMPLE Archieve/SIMPLE-db/scripts/ingests/Zhang18/sty2054_supplemental_files"

csv_file = "/Users/guanying/SIMPLE Archieve/SIMPLE-db/scripts/ingests/Zhang18/zhang18_spectra.csv"

csv_rows = []

for filename in os.listdir(path):
    if not filename.endswith(".fits"):
        continue

    file_path = os.path.join(path, filename)
    print(f"\nReading: {filename}")

    sourceName = "_".join(filename.split("_")[0:2])

    with fits.open(file_path) as hdul:
        header = hdul[0].header

        # Get metadata from header
        instrument_ = header.get("INSTRUME", None)
        telescope_ = header.get("TELESCOP", None)
        obs_date_ = header.get("DATE-OBS", None)
        mode_ = header.get("INSMODE", None)

        # Find missing values from header
        if (instrument_ == "XSHOOTER") or "xshooter" in filename.lower():
            instrument_ = "XShooter"
            telescope_ = "ESO VLT"
            mode_ = "Echelle"
        elif instrument_ == "OSIRIS" or "osiris" in filename.lower():
            instrument_ = "OSIRIS"
            telescope_ = "GTC"
            mode_ = "Missing"

        # Some of the mode(regime) is not available in the header
        # VIS: range 0.559.5-1.024 µm --- optical
        # NIR: 1.024-2.480 µm --- nir
        # UVB: 0.300-0.559.5 µm -- optical
        # OSIRIS: 0.365 to 1.05 µm --- optical

        filename_lower = filename.lower()
        if instrument_ == "XShooter":
            if "nir" in filename_lower:
                regime = "nir"
            elif "vis" in filename_lower or "uvb" in filename_lower:
                regime = "optical"
        elif instrument_ == "OSIRIS":
            regime = "optical"

        # Some date format is not standard
        try:
            obs_date = Time(obs_date_).to_datetime().isoformat()
        except Exception:
            obs_date = None

        # prepare rows for csv file
        csv_rows.append({
            "source": sourceName,
            "access_url": file_path,
            "regime": regime,
            "telescope": telescope_,
            "instrument": instrument_,
            "mode": mode_,
            "observation_date": obs_date,
            "comments": None,
            "reference": "Zhan18.1352"
        })

# Create csv file
with open(csv_file, mode='w', newline='') as file:
    fieldnames = [
        "source", 
        "access_url", 
        "regime", 
        "telescope",
        "instrument", 
        "mode", 
        "observation_date", 
        "comments", 
        "reference"
    ]
    writer = csv.DictWriter(file, fieldnames=csv_rows[0].keys())
    writer.writeheader()
    writer.writerows(csv_rows)

print(f"CSV file '{csv_file}' created with {len(csv_rows)} entries.")
        

