"""Safe firmware update dispatcher for Skybrush Server."""

from .extension import FirmwareUpdateExtension, schema

__all__ = ("construct", "description", "schema")

construct = FirmwareUpdateExtension
description = "Safe server-side firmware update dispatcher"

