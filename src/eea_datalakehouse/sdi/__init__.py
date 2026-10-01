"""eea_datalakehouse.sdi — ISO 19115-3 metadata from the EEA SDI catalogue to DDS.

Typical use (``SDI_API_URL``, ``DDS_BASE_URL`` and the Dremio credentials come
from the kernel env)::

    from eea_datalakehouse.sdi import SdiController

    with SdiController.from_env() as sdi:
        metadata = sdi.get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")
        result = sdi.push_to_dds(metadata, "water_management_resources/bathing_water/bwd")
        result.dds_path   # ".../bwd/metadata/070d9baa-448d-4168-8514-7dadb3ad876d.xml"
        result.action     # "uploaded" | "unchanged" | "replaced"
"""

from __future__ import annotations

from .catalogue import SdiCatalogue
from .config import DEFAULT_SDI_API_URL, SdiConfig
from .controller import (
    DEFAULT_FOLDER,
    PushResult,
    SdiController,
    SdiMetadata,
    metadata_path,
)
from .errors import (
    DdsCopyConflict,
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
    "DEFAULT_FOLDER",
    "DEFAULT_SDI_API_URL",
    "DdsCopyConflict",
    "IsoRecord",
    "NotCurrentError",
    "NotIso19115_3",
    "PushResult",
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
    "metadata_path",
]
