import logging
import sys
from .version import version as __version__

__all__ = ["__version__"]

REFERENCE_TABLES = [
    "Publications",
    "Telescopes",
    "Instruments",
    "Modes",
    "PhotometryFilters",
    "Versions",
    "Parameters",
    "Regimes",
]

logger = logging.getLogger(__name__)

LOGFORMAT = logging.Formatter("%(levelname)-8s - %(name)-15s - %(message)s")

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(LOGFORMAT)
logger.addHandler(handler)

logger.setLevel(logging.INFO)
logger.info("SIMPLE logger initialized")
logger.info(f"Logger level: {logging.getLevelName(logger.getEffectiveLevel()) }")
