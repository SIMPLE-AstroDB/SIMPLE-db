import numpy as np
from astroquery.gaia import Gaia
import logging
from simple.utils.astrometry import ingest_parallaxes, ingest_proper_motions

# from scripts.utils.photometry import ingest_photometry
# used by broken ingest_gaia_photometry

__all__ = [
    "get_gaiadr3",
    "ingest_gaia_photometry",
    "ingest_gaia_parallaxes",
    "ingest_gaia_pms",
]

logger = logging.getLogger("SIMPLE")


def get_gaiadr3(gaia_id, verbose=True):
    """
    Currently setup just to query one source
    TODO: add some debug and info messages

    Parameters
    ----------
    gaia_id: str or int
    verbose

    Returns
    -------
    Table of Gaia data

    """
    gaia_query_string = (
        f"SELECT "
        f"parallax, parallax_error, "
        f"pmra, pmra_error, pmdec, pmdec_error, "
        f"phot_g_mean_flux, phot_g_mean_flux_error, phot_g_mean_mag, "
        f"phot_rp_mean_flux, phot_rp_mean_flux_error, phot_rp_mean_mag "
        f"FROM gaiadr3.gaia_source WHERE "
        f"gaiadr3.gaia_source.source_id = '{gaia_id}'"
    )
    job_gaia_query = Gaia.launch_job(gaia_query_string, verbose=verbose)

    gaia_data = job_gaia_query.get_results()

    return gaia_data


def ingest_gaia_photometry(db, sources, gaia_data, ref):
    # TODO write some tests
    unmasked_gphot = np.logical_not(gaia_data["phot_g_mean_mag"].mask).nonzero()
    gaia_g_phot = gaia_data[unmasked_gphot][
        "phot_g_mean_flux", "phot_g_mean_flux_error", "phot_g_mean_mag"
    ]

    unmased_rpphot = np.logical_not(gaia_data["phot_rp_mean_mag"].mask).nonzero()
    gaia_rp_phot = gaia_data[unmased_rpphot][
        "phot_rp_mean_flux", "phot_rp_mean_flux_error", "phot_rp_mean_mag"
    ]

    # e_Gmag=abs(-2.5/ln(10)*e_FG/FG) from Vizier Note 37 on Gaia DR2 (I/345/gaia2)
    gaia_g_phot["g_unc"] = np.abs(
        -2.5
        / np.log(10)
        * gaia_g_phot["phot_g_mean_flux_error"]
        / gaia_g_phot["phot_g_mean_flux"]
    )
    gaia_rp_phot["rp_unc"] = np.abs(
        -2.5
        / np.log(10)
        * gaia_rp_phot["phot_rp_mean_flux_error"]
        / gaia_rp_phot["phot_rp_mean_flux"]
    )

    if ref == "GaiaDR2":
        g_band_name = "GAIA2.G"
        rp_band_name = "GAIA2.Grp"
    elif ref == "GaiaEDR3" or ref == "GaiaDR3":
        g_band_name = "GAIA3.G"
        rp_band_name = "GAIA3.Grp"
    else:
        raise Exception

    print(g_band_name, rp_band_name)

    # TODO: Turn into a loop and ingest one at a time
    # ingest_photometry(
    #     db,
    #     sources,
    #     g_band_name,
    #     gaia_g_phot["phot_g_mean_mag"],
    #     gaia_g_phot["g_unc"],
    #     ref,
    #     ucds="em.opt",
    #     telescope="Gaia",
    #     instrument="Gaia",
    # )

    # ingest_photometry(
    #     db,
    #     sources,
    #     rp_band_name,
    #     gaia_rp_phot["phot_rp_mean_mag"],
    #     gaia_rp_phot["rp_unc"],
    #     ref,
    #     ucds="em.opt.R",
    #     telescope="Gaia",
    #     instrument="Gaia",
    # )

    return


def ingest_gaia_parallaxes(db, sources, gaia_data, ref):
    # TODO write some tests
    unmasked_pi = np.logical_not(gaia_data["parallax"].mask).nonzero()
    gaia_parallaxes = gaia_data[unmasked_pi]["parallax", "parallax_error"]

    ingest_parallaxes(
        db, sources, gaia_parallaxes["parallax"], gaia_parallaxes["parallax_error"], ref
    )


def ingest_gaia_pms(db, sources, gaia_data, ref):
    # TODO write some tests
    unmasked_pms = np.logical_not(gaia_data["pmra"].mask).nonzero()
    pms = gaia_data[unmasked_pms]["pmra", "pmra_error", "pmdec", "pmdec_error"]
    refs = [ref] * len(pms)

    ingest_proper_motions(
        db,
        sources,
        pms["pmra"],
        pms["pmra_error"],
        pms["pmdec"],
        pms["pmdec_error"],
        refs,
    )
