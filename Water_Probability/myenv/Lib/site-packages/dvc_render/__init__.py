"""
Library for rendering DVC plots
"""

from .html import render_html  # noqa: F401
from .image import ImageRenderer
from .plotly import ParallelCoordinatesRenderer  # noqa: F401
from .vega import VegaRenderer
from .vega_templates import TEMPLATES  # noqa: F401

RENDERERS = [ImageRenderer, VegaRenderer]
