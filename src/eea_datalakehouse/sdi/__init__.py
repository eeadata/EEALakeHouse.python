"""eea_datalakehouse.sdi — ISO 19115-3 metadata from the EEA SDI catalogue.

Typical use (``SDI_API_URL`` comes from the kernel env; pushing to DDS is
:mod:`eea_datalakehouse.dds_documents`'s job)::

    from eea_datalakehouse.dds_documents import DdsController, DdsFolder
    from eea_datalakehouse.sdi import SdiController

    with SdiController.from_env() as sdi, DdsController.from_env() as dds:
        metadata = sdi.get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")
        dds.push("catalog/water_management_resources/bathing_water/bwd",
                 DdsFolder.METADATA, metadata)
        # -> .../bwd/metadata/070d9baa-448d-4168-8514-7dadb3ad876d.xml
"""

from __future__ import annotations

from .catalogue import SdiCatalogue
from .config import DEFAULT_SDI_API_URL, SdiConfig
from .controller import SdiController, SdiMetadata
from .errors import (
    NotCurrentError,
    NotIso19115_3,
    SdiApiError,
    SdiAuthError,
    SdiError,
    SdiNotFound,
    SeriesCandidate,
    UuidMismatch,
)
from .iso import IsoRecord
from .session import SdiSession, SdiSessionError

__all__ = [
    "DEFAULT_SDI_API_URL",
    "IsoRecord",
    "NotCurrentError",
    "NotIso19115_3",
    "SdiApiError",
    "SdiAuthError",
    "SdiCatalogue",
    "SdiConfig",
    "SdiController",
    "SdiError",
    "SdiMetadata",
    "SdiNotFound",
    "SdiSession",
    "SdiSessionError",
    "SeriesCandidate",
    "UuidMismatch",
]
