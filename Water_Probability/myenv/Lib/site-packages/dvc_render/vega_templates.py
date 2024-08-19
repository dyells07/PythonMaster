# pylint: disable=missing-function-docstring
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from .exceptions import DvcRenderError

if TYPE_CHECKING:
    from .base import StrPath


class TemplateNotFoundError(DvcRenderError):
    def __init__(self, path):
        super().__init__(f"Template '{path}' not found.")


class NoFieldInDataError(DvcRenderError):
    def __init__(self, field_name):
        super().__init__(f"Field '{field_name}' does not exist in provided data.")


class TemplateContentDoesNotMatchError(DvcRenderError):
    def __init__(self, template_name: str, path: str):
        super().__init__(
            f"Template '{path}' already exists "
            f"and its content is different than '{template_name}' content. "
            "Remove it manually if you want to recreate it."
        )


class BadTemplateError(DvcRenderError):
    pass


def dict_replace_value(d: dict, name: str, value: Any) -> dict:
    x = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = dict_replace_value(v, name, value)
        elif isinstance(v, list):
            v = list_replace_value(v, name, value)
        elif isinstance(v, str):
            if v == name:
                x[k] = value
                continue
            if name in v and isinstance(value, str):
                v = v.replace(name, value)
        x[k] = v
    return x


def list_replace_value(l: list, name: str, value: str) -> list:  # noqa: E741
    x = []
    for e in l:
        if isinstance(e, list):
            e = list_replace_value(e, name, value)
        elif isinstance(e, dict):
            e = dict_replace_value(e, name, value)
        elif isinstance(e, str) and e == name:
            e = value
        x.append(e)
    return x


def find_value(d: Union[dict, list, str], value: str) -> bool:
    if isinstance(d, dict):
        for v in d.values():
            if isinstance(v, dict) and find_value(v, value):
                return True
            if isinstance(v, str) and v == value:
                return True
            if isinstance(v, list) and any(find_value(e, value) for e in v):
                return True
    elif isinstance(d, str) and d == value:
        return True
    return False


class Template:
    EXTENSION = ".json"
    ANCHOR = "<DVC_METRIC_{}>"

    DEFAULT_CONTENT: dict[str, Any] = {}
    DEFAULT_NAME: str = ""

    def __init__(
        self, content: Optional[dict[str, Any]] = None, name: Optional[str] = None
    ):
        if (
            content
            and not isinstance(content, dict)
            or self.DEFAULT_CONTENT
            and not isinstance(self.DEFAULT_CONTENT, dict)
        ):
            raise BadTemplateError
        self._original_content = content or self.DEFAULT_CONTENT
        self.content: dict[str, Any] = self._original_content
        self.name = name or self.DEFAULT_NAME
        self.filename = Path(self.name).with_suffix(self.EXTENSION)

    @classmethod
    def anchor(cls, name):
        "Get ANCHOR formatted with name."
        return cls.ANCHOR.format(name.upper())

    @classmethod
    def escape_special_characters(cls, value: str) -> str:
        "Escape special characters in `value`"
        for character in (".", "[", "]"):
            value = value.replace(character, "\\" + character)
        return value

    @staticmethod
    def check_field_exists(data, field):
        "Raise NoFieldInDataError if `field` not in `data`."
        if not any(field in row for row in data):
            raise NoFieldInDataError(field)

    def reset(self):
        """Reset self.content to its original state."""
        self.content = self._original_content

    def has_anchor(self, name) -> bool:
        "Check if ANCHOR formatted with name is in content."
        return find_value(self.content, self.anchor(name))

    def fill_anchor(self, name, value) -> None:
        "Replace anchor `name` with `value` in content."
        self.content = dict_replace_value(self.content, self.anchor(name), value)


class BarHorizontalSortedTemplate(Template):
    DEFAULT_NAME = "bar_horizontal_sorted"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": Template.anchor("data")},
        "title": {"text": Template.anchor("title"), "anchor": "middle"},
        "width": Template.anchor("plot_width"),
        "height": Template.anchor("plot_height"),
        "mark": {"type": "bar"},
        "params": [Template.anchor("zoom_and_pan")],
        "encoding": {
            "x": {
                "field": Template.anchor("x"),
                "type": "quantitative",
                "title": Template.anchor("x_label"),
                "scale": {"zero": False},
            },
            "y": {
                "field": Template.anchor("y"),
                "type": "nominal",
                "title": Template.anchor("y_label"),
                "sort": "-x",
            },
            "yOffset": {"field": "rev", "sort": []},
            "color": Template.anchor("color"),
            "column": Template.anchor("column"),
        },
    }


class BarHorizontalTemplate(Template):
    DEFAULT_NAME = "bar_horizontal"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": Template.anchor("data")},
        "title": {"text": Template.anchor("title"), "anchor": "middle"},
        "width": Template.anchor("plot_width"),
        "height": Template.anchor("plot_height"),
        "mark": {"type": "bar"},
        "params": [Template.anchor("zoom_and_pan")],
        "encoding": {
            "x": {
                "field": Template.anchor("x"),
                "type": "quantitative",
                "title": Template.anchor("x_label"),
                "scale": {"zero": False},
            },
            "y": {
                "field": Template.anchor("y"),
                "type": "nominal",
                "title": Template.anchor("y_label"),
            },
            "yOffset": {"field": "rev", "sort": []},
            "color": Template.anchor("color"),
            "column": Template.anchor("column"),
        },
    }


class ConfusionTemplate(Template):
    DEFAULT_NAME = "confusion"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": Template.anchor("data")},
        "title": {"text": Template.anchor("title"), "anchor": "middle"},
        "facet": {
            "column": {"field": "rev", "sort": []},
            "row": Template.anchor("row"),
        },
        "params": [
            {
                "name": "showValues",
                "bind": {"input": "checkbox"},
            },
        ],
        "spec": {
            "transform": [
                {
                    "aggregate": [{"op": "count", "as": "xy_count"}],
                    "groupby": [Template.anchor("y"), Template.anchor("x")],
                },
                {
                    "impute": "xy_count",
                    "groupby": Template.anchor("group_by_y"),
                    "key": Template.anchor("x"),
                    "value": 0,
                },
                {
                    "impute": "xy_count",
                    "groupby": Template.anchor("group_by_x"),
                    "key": Template.anchor("y"),
                    "value": 0,
                },
                {
                    "joinaggregate": [
                        {"op": "max", "field": "xy_count", "as": "max_count"}
                    ],
                    "groupby": [],
                },
                {
                    "calculate": "datum.xy_count / datum.max_count",
                    "as": "percent_of_max",
                },
            ],
            "encoding": {
                "x": {
                    "field": Template.anchor("x"),
                    "type": "nominal",
                    "sort": "ascending",
                    "title": Template.anchor("x_label"),
                },
                "y": {
                    "field": Template.anchor("y"),
                    "type": "nominal",
                    "sort": "ascending",
                    "title": Template.anchor("y_label"),
                },
            },
            "layer": [
                {
                    "mark": "rect",
                    "width": Template.anchor("plot_width"),
                    "height": Template.anchor("plot_height"),
                    "encoding": {
                        "color": {
                            "field": "xy_count",
                            "type": "quantitative",
                            "title": "",
                            "scale": {"domainMin": 0, "nice": True},
                        }
                    },
                },
                {
                    "selection": {
                        "label": {
                            "type": "single",
                            "on": "mouseover",
                            "encodings": ["x", "y"],
                            "empty": "none",
                            "clear": "mouseout",
                        }
                    },
                    "mark": "rect",
                    "encoding": {
                        "tooltip": [
                            {"field": Template.anchor("x"), "type": "nominal"},
                            {"field": Template.anchor("y"), "type": "nominal"},
                            {"field": "xy_count", "type": "quantitative"},
                        ],
                        "opacity": {
                            "condition": {"selection": "label", "value": 1},
                            "value": 0,
                        },
                    },
                },
                {
                    "transform": [{"filter": {"selection": "label"}}],
                    "layer": [
                        {"mark": {"type": "rect", "color": "lightpink"}},
                    ],
                },
                {
                    "mark": "text",
                    "encoding": {
                        "text": {
                            "condition": {
                                "param": "showValues",
                                "field": "xy_count",
                                "type": "quantitative",
                            },
                        },
                        "color": {
                            "condition": {
                                "test": "datum.percent_of_max > 0.5",
                                "value": "white",
                            },
                            "value": "black",
                        },
                    },
                },
            ],
        },
    }


class NormalizedConfusionTemplate(Template):
    DEFAULT_NAME = "confusion_normalized"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": Template.anchor("data")},
        "title": {"text": Template.anchor("title"), "anchor": "middle"},
        "facet": {
            "column": {"field": "rev", "sort": []},
            "row": Template.anchor("row"),
        },
        "spec": {
            "transform": [
                {
                    "aggregate": [{"op": "count", "as": "xy_count"}],
                    "groupby": [Template.anchor("y"), Template.anchor("x")],
                },
                {
                    "impute": "xy_count",
                    "groupby": ["rev", Template.anchor("y")],
                    "key": Template.anchor("x"),
                    "value": 0,
                },
                {
                    "impute": "xy_count",
                    "groupby": Template.anchor("group_by_x"),
                    "key": Template.anchor("y"),
                    "value": 0,
                },
                {
                    "joinaggregate": [
                        {"op": "sum", "field": "xy_count", "as": "sum_y"}
                    ],
                    "groupby": [Template.anchor("y")],
                },
                {
                    "calculate": "datum.xy_count / datum.sum_y",
                    "as": "percent_of_y",
                },
            ],
            "encoding": {
                "x": {
                    "field": Template.anchor("x"),
                    "type": "nominal",
                    "sort": "ascending",
                    "title": Template.anchor("x_label"),
                },
                "y": {
                    "field": Template.anchor("y"),
                    "type": "nominal",
                    "sort": "ascending",
                    "title": Template.anchor("y_label"),
                },
            },
            "layer": [
                {
                    "mark": "rect",
                    "width": Template.anchor("plot_width"),
                    "height": Template.anchor("plot_height"),
                    "encoding": {
                        "color": {
                            "field": "percent_of_y",
                            "type": "quantitative",
                            "title": "",
                            "scale": {"domain": [0, 1]},
                        }
                    },
                },
                {
                    "selection": {
                        "label": {
                            "type": "single",
                            "on": "mouseover",
                            "encodings": ["x", "y"],
                            "empty": "none",
                            "clear": "mouseout",
                        }
                    },
                    "mark": "rect",
                    "encoding": {
                        "tooltip": [
                            {"field": Template.anchor("x"), "type": "nominal"},
                            {"field": Template.anchor("y"), "type": "nominal"},
                            {
                                "field": "percent_of_y",
                                "type": "quantitative",
                                "format": ".2f",
                            },
                        ],
                        "opacity": {
                            "condition": {"selection": "label", "value": 1},
                            "value": 0,
                        },
                    },
                },
                {
                    "transform": [{"filter": {"selection": "label"}}],
                    "layer": [
                        {"mark": {"type": "rect", "color": "lightpink"}},
                    ],
                },
                {
                    "mark": "text",
                    "encoding": {
                        "color": {
                            "condition": {
                                "test": "datum.percent_of_y > 0.5",
                                "value": "white",
                            },
                            "value": "black",
                        },
                    },
                },
            ],
        },
    }


class ScatterTemplate(Template):
    DEFAULT_NAME = "scatter"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": Template.anchor("data")},
        "title": {"text": Template.anchor("title"), "anchor": "middle"},
        "width": Template.anchor("plot_width"),
        "height": Template.anchor("plot_height"),
        "mark": {"type": "point", "tooltip": {"content": "data"}},
        "params": [Template.anchor("zoom_and_pan")],
        "encoding": {
            "x": {
                "field": Template.anchor("x"),
                "type": "quantitative",
                "title": Template.anchor("x_label"),
            },
            "y": {
                "field": Template.anchor("y"),
                "type": "quantitative",
                "title": Template.anchor("y_label"),
            },
            "color": Template.anchor("color"),
            "shape": Template.anchor("shape"),
            "tooltip": Template.anchor("tooltip"),
        },
    }


class ScatterJitterTemplate(Template):
    DEFAULT_NAME = "scatter_jitter"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": Template.anchor("data")},
        "title": {"text": Template.anchor("title"), "anchor": "middle"},
        "width": Template.anchor("plot_width"),
        "height": Template.anchor("plot_height"),
        "transform": [
            {"calculate": "random()", "as": "randomX"},
            {"calculate": "random()", "as": "randomY"},
        ],
        "mark": {"type": "point", "tooltip": {"content": "data"}},
        "encoding": {
            "x": {
                "field": Template.anchor("x"),
                "title": Template.anchor("x_label"),
            },
            "y": {
                "field": Template.anchor("y"),
                "title": Template.anchor("y_label"),
            },
            "color": Template.anchor("color"),
            "shape": Template.anchor("shape"),
            "tooltip": Template.anchor("tooltip"),
            "xOffset": {"field": "randomX", "type": "quantitative"},
            "yOffset": {"field": "randomY", "type": "quantitative"},
        },
    }


class SmoothLinearTemplate(Template):
    DEFAULT_NAME = "smooth"
    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": Template.anchor("data")},
        "title": {"text": Template.anchor("title"), "anchor": "middle"},
        "width": Template.anchor("plot_width"),
        "height": Template.anchor("plot_height"),
        "params": [
            {
                "name": "smooth",
                "value": 0.001,
                "bind": {
                    "input": "range",
                    "min": 0.001,
                    "max": 1,
                    "step": 0.001,
                },
            },
        ],
        "encoding": {
            "x": {
                "field": Template.anchor("x"),
                "type": "quantitative",
                "title": Template.anchor("x_label"),
            },
            "color": Template.anchor("color"),
            "strokeDash": Template.anchor("stroke_dash"),
        },
        "layer": [
            {
                "layer": [
                    {"params": [Template.anchor("zoom_and_pan")], "mark": "line"},
                    {
                        "transform": [{"filter": {"param": "hover", "empty": False}}],
                        "mark": "point",
                    },
                ],
                "encoding": {
                    "y": {
                        "field": Template.anchor("y"),
                        "type": "quantitative",
                        "title": Template.anchor("y_label"),
                        "scale": {"zero": False},
                    },
                    "color": {
                        "field": "rev",
                        "type": "nominal",
                    },
                },
                "transform": [
                    {
                        "loess": Template.anchor("y"),
                        "on": Template.anchor("x"),
                        "groupby": Template.anchor("group_by"),
                        "bandwidth": {"signal": "smooth"},
                    },
                ],
            },
            {
                "mark": {"type": "line", "opacity": 0.2},
                "encoding": {
                    "x": {
                        "field": Template.anchor("x"),
                        "type": "quantitative",
                        "title": Template.anchor("x_label"),
                    },
                    "y": {
                        "field": Template.anchor("y"),
                        "type": "quantitative",
                        "title": Template.anchor("y_label"),
                        "scale": {"zero": False},
                    },
                    "color": {"field": "rev", "type": "nominal"},
                },
            },
            {
                "mark": {"type": "circle", "size": 10},
                "encoding": {
                    "x": {
                        "aggregate": "max",
                        "field": Template.anchor("x"),
                        "type": "quantitative",
                        "title": Template.anchor("x_label"),
                    },
                    "y": {
                        "aggregate": {"argmax": Template.anchor("x")},
                        "field": Template.anchor("y"),
                        "type": "quantitative",
                        "title": Template.anchor("y_label"),
                        "scale": {"zero": False},
                    },
                    "color": {"field": "rev", "type": "nominal"},
                },
            },
            {
                "transform": [
                    {"calculate": Template.anchor("pivot_field"), "as": "pivot_field"},
                    {
                        "pivot": "pivot_field",
                        "op": "mean",
                        "value": Template.anchor("y"),
                        "groupby": [Template.anchor("x")],
                    },
                ],
                "mark": {
                    "type": "rule",
                    "tooltip": {"content": "data"},
                    "stroke": "grey",
                },
                "encoding": {
                    "opacity": {
                        "condition": {"value": 0.3, "param": "hover", "empty": False},
                        "value": 0,
                    }
                },
                "params": [
                    {
                        "name": "hover",
                        "select": {
                            "type": "point",
                            "fields": [Template.anchor("x")],
                            "nearest": True,
                            "on": "mouseover",
                            "clear": "mouseout",
                        },
                    }
                ],
            },
        ],
    }


class LinearTemplate(SmoothLinearTemplate):
    DEFAULT_NAME = "linear"


class SimpleLinearTemplate(Template):
    DEFAULT_NAME = "simple"

    DEFAULT_CONTENT = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": Template.anchor("data")},
        "title": {"text": Template.anchor("title"), "anchor": "middle"},
        "params": [Template.anchor("zoom_and_pan")],
        "width": Template.anchor("plot_width"),
        "height": Template.anchor("plot_height"),
        "mark": {
            "type": "line",
            "tooltip": {"content": "data"},
        },
        "encoding": {
            "x": {
                "field": Template.anchor("x"),
                "type": "quantitative",
                "title": Template.anchor("x_label"),
            },
            "y": {
                "field": Template.anchor("y"),
                "type": "quantitative",
                "title": Template.anchor("y_label"),
                "scale": {"zero": False},
            },
            "color": Template.anchor("color"),
            "strokeDash": Template.anchor("stroke_dash"),
            "tooltip": Template.anchor("tooltip"),
        },
    }


TEMPLATES = [
    SimpleLinearTemplate,
    LinearTemplate,
    ConfusionTemplate,
    NormalizedConfusionTemplate,
    ScatterTemplate,
    ScatterJitterTemplate,
    SmoothLinearTemplate,
    BarHorizontalSortedTemplate,
    BarHorizontalTemplate,
]


def _find_template(
    template_name: str, template_dir: Optional[str] = None, fs=None
) -> Optional["StrPath"]:
    _exists = Path.exists if fs is None else fs.exists

    if template_dir:
        template_path = Path(template_dir) / template_name
        if _exists(template_path):
            return template_path
        if _exists(template_path.with_suffix(Template.EXTENSION)):
            return template_path.with_suffix(Template.EXTENSION)

    template_path = Path(template_name)
    if _exists(template_path):
        return template_path.resolve()

    return None


def get_template(
    template: Union[Optional[str], Template] = None,
    template_dir: Optional[str] = None,
    fs=None,
) -> Template:
    """Return template instance based on given template arg.

    If template is already an instance, return it.
    If template is None, return default `linear` template.
    If template is a path, will try to find it:
        - Inside `template_dir`
        - As a relative path to cwd.
    If template matches one of the DEFAULT_NAMEs in TEMPLATES,
    return an instance of the one matching.
    """
    if isinstance(template, Template):
        return template

    if template is None:
        template = "linear"

    template_path = _find_template(template, template_dir, fs)

    _open = open if fs is None else fs.open
    if template_path:
        with _open(template_path, encoding="utf-8") as f:
            content = json.load(f)
        return Template(content, name=template)

    for template_cls in TEMPLATES:
        if template == template_cls.DEFAULT_NAME:
            return template_cls()

    raise TemplateNotFoundError(template)


def dump_templates(output: "StrPath", targets: Optional[list] = None) -> None:
    "Write TEMPLATES in `.json` format to `output`."
    output = Path(output)
    output.mkdir(exist_ok=True)

    if targets:
        templates = [
            template for template in TEMPLATES if template.DEFAULT_NAME in targets
        ]
    else:
        templates = TEMPLATES

    for template_cls in templates:
        template = template_cls()
        path = output / template.filename

        if path.exists():
            content = path.read_text(encoding="utf-8")
            if content != template.content:
                raise TemplateContentDoesNotMatchError(template.DEFAULT_NAME, str(path))
        else:
            path.write_text(json.dumps(template.content), encoding="utf-8")
