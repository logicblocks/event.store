"""The references package.

See [`Widget`][acme.kit.surface.widgets.Widget] and
[`Sprocket`][acme.kit.surface.nested.Sprocket], but not
[`Missing`][acme.kit.surface.Missing] or [`empty`][acme.kit.empty].
"""

from datetime import datetime

import structlog
from acme.kit.surface import Widget
from acme.kit.surface.nested import Gadget
from acme.kit.surface.nested.parts import Sprocket

from .reports import Report


def summarise(
    widget: Widget,
    gadget: Gadget,
    sprocket: Sprocket,
    at: datetime,
    logger: structlog.BoundLogger,
) -> Report:
    """Summarises.

    Args:
        widget: The widget.

    Raises:
        LookupError: If nothing is found.
    """
    return Report()


__all__ = ["Report", "Widget", "summarise"]
