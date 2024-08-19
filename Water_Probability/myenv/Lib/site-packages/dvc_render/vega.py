import base64
import io
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional, Union
from warnings import warn

from .base import Renderer
from .utils import list_dict_to_dict_list
from .vega_templates import BadTemplateError, LinearTemplate, Template, get_template

FIELD_SEPARATOR = "::"
REV = "rev"
FILENAME = "filename"
FIELD = "field"
FILENAME_FIELD = [FILENAME, FIELD]
CONCAT_FIELDS = FIELD_SEPARATOR.join(FILENAME_FIELD)

SPLIT_ANCHORS = [
    "color",
    "data",
    "plot_height",
    "plot_width",
    "shape",
    "stroke_dash",
    "title",
    "tooltip",
    "x_label",
    "y_label",
    "zoom_and_pan",
]
OPTIONAL_ANCHORS = [
    "color",
    "column",
    "group_by_x",
    "group_by_y",
    "group_by",
    "pivot_field",
    "plot_height",
    "plot_width",
    "row",
    "shape",
    "stroke_dash",
    "tooltip",
    "zoom_and_pan",
]
OPTIONAL_ANCHOR_RANGES: dict[str, Union[list[str], list[list[int]]]] = {
    "stroke_dash": [[1, 0], [8, 8], [8, 4], [4, 4], [4, 2], [2, 1], [1, 1]],
    "color": [
        "#945dd6",
        "#13adc7",
        "#f46837",
        "#48bb78",
        "#4299e1",
        "#ed8936",
        "#f56565",
    ],
    "shape": ["circle", "square", "triangle", "diamond"],
}


class VegaRenderer(Renderer):
    """Renderer for vega plots."""

    TYPE = "vega"

    DIV = """
    <div id = "{id}">
        <script type = "text/javascript">
            var spec = {partial};
            vegaEmbed('#{id}', spec);
        </script>
    </div>
    """

    SCRIPTS = """
    <script src="https://cdn.jsdelivr.net/npm/vega@5.20.2"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5.2.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.18.2"></script>
    """

    EXTENSIONS = {".yml", ".yaml", ".json", ".csv", ".tsv"}

    def __init__(self, datapoints: list, name: str, **properties):
        super().__init__(datapoints, name, **properties)
        self.template = get_template(
            self.properties.get("template", None),
            self.properties.get("template_dir", None),
        )

        self._split_content: dict[str, str] = {}

    def get_filled_template(  # noqa: C901
        self,
        split_anchors: Optional[list[str]] = None,
        strict: bool = True,
    ) -> dict[str, Any]:
        """Returns a functional vega specification"""
        self.template.reset()
        if not self.datapoints:
            return {}

        if split_anchors is None:
            split_anchors = []

        if strict:
            if self.properties.get("x"):
                self.template.check_field_exists(
                    self.datapoints, self.properties.get("x")
                )
            if self.properties.get("y"):
                self.template.check_field_exists(
                    self.datapoints, self.properties.get("y")
                )
        self.properties.setdefault("title", "")
        self.properties.setdefault("x_label", self.properties.get("x"))
        self.properties.setdefault("y_label", self.properties.get("y"))
        self.properties.setdefault("data", self.datapoints)

        varied_keys = self._process_optional_anchors(split_anchors)
        self._update_datapoints(varied_keys)

        names = ["title", "x", "y", "x_label", "y_label", "data"]
        for name in names:
            value = self.properties.get(name)
            if value is None:
                continue

            if name in split_anchors:
                self._set_split_content(name, value)
                continue

            if name == "data":
                if not self.template.has_anchor(name):
                    anchor = self.template.anchor(name)
                    raise BadTemplateError(  # noqa: TRY003
                        f"Template '{self.template.name}' "
                        f"is not using '{anchor}' anchor"
                    )
            elif name in {"x", "y"}:
                value = self.template.escape_special_characters(value)
            self.template.fill_anchor(name, value)

        return self.template.content

    def get_partial_filled_template(self):
        """
        Returns a partially filled template along with the split out anchor content
        """
        content = self.get_filled_template(
            split_anchors=SPLIT_ANCHORS,
            strict=True,
        )
        return content, {"anchor_definitions": self._split_content}

    def get_template(self):
        """
        Returns unfilled template (for Studio)
        """
        return self.template.content

    def partial_html(self, **kwargs) -> str:  # noqa: ARG002
        content = self.get_filled_template()
        return json.dumps(content)

    def generate_markdown(self, report_path=None) -> str:
        if not isinstance(self.template, LinearTemplate):
            warn("`generate_markdown` can only be used with `LinearTemplate`")  # noqa: B028
            return ""
        try:
            from matplotlib import pyplot as plt
        except ImportError as e:
            raise ImportError("matplotlib is required for `generate_markdown`") from e  # noqa: TRY003

        data = list_dict_to_dict_list(self.datapoints)
        if data:
            if report_path:
                report_folder = Path(report_path).parent
                output_file = report_folder / self.name
                output_file = output_file.with_suffix(".png")
                output_file.parent.mkdir(exist_ok=True, parents=True)
            else:
                output_file = io.BytesIO()  # type: ignore[assignment]

            x = self.properties.get("x")
            y = self.properties.get("y")
            data[x] = list(map(float, data[x]))
            data[y] = list(map(float, data[y]))

            if x is not None and y is not None:
                plt.title(self.properties.get("title", Path(self.name).stem))
                plt.xlabel(self.properties.get("x_label", x))
                plt.ylabel(self.properties.get("y_label", y))
                plt.plot(x, y, data=data)
                plt.tight_layout()
                plt.savefig(output_file)
                plt.close()

                if report_path:
                    return f"\n![{self.name}]({output_file.relative_to(report_folder)})"

                base64_str = base64.b64encode(output_file.getvalue()).decode()  # type: ignore[attr-defined]
                src = f"data:image/png;base64,{base64_str}"

                return f"\n![{self.name}]({src})"

        return ""

    def get_revs(self):
        """
        Returns all revisions that were collected that have datapoints.
        """
        return (
            self.properties.get("revs_with_datapoints", [])
            or self._get_revs_from_datapoints()
        )

    def _get_revs_from_datapoints(self):
        revs = []
        for datapoint in self.datapoints:
            rev = datapoint.get("rev")
            if rev and rev not in revs:
                revs.append(rev)
        return revs

    def _process_optional_anchors(self, split_anchors: list[str]):
        optional_anchors = [
            anchor for anchor in OPTIONAL_ANCHORS if self.template.has_anchor(anchor)
        ]
        if not optional_anchors:
            return None

        self._fill_color(split_anchors, optional_anchors)
        self._fill_set_encoding(split_anchors, optional_anchors)

        y_definitions = self.properties.get("anchors_y_definitions", [])
        is_single_source = len(y_definitions) <= 1

        if is_single_source:
            self._process_single_source_plot(split_anchors, optional_anchors)
            return []

        return self._process_multi_source_plot(
            split_anchors, optional_anchors, y_definitions
        )

    def _fill_color(self, split_anchors: list[str], optional_anchors: list[str]):
        all_revs = self.get_revs()
        self._fill_optional_anchor_mapping(
            split_anchors,
            optional_anchors,
            REV,
            "color",
            all_revs,
        )

    def _fill_set_encoding(self, split_anchors: list[str], optional_anchors: list[str]):
        for name, encoding in [
            ("zoom_and_pan", {"name": "grid", "select": "interval", "bind": "scales"}),
            ("plot_height", 300),
            ("plot_width", 300),
        ]:
            self._fill_optional_anchor(split_anchors, optional_anchors, name, encoding)

    def _process_single_source_plot(
        self, split_anchors: list[str], optional_anchors: list[str]
    ):
        self._fill_group_by(split_anchors, optional_anchors, [REV])
        self._fill_optional_anchor(
            split_anchors, optional_anchors, "pivot_field", "datum.rev"
        )
        self._fill_tooltip(split_anchors, optional_anchors)
        for anchor in optional_anchors:
            self.template.fill_anchor(anchor, {})

    def _process_multi_source_plot(
        self,
        split_anchors: list[str],
        optional_anchors: list[str],
        y_definitions: list[dict[str, str]],
    ):
        varied_keys, domain = self._collect_variations(y_definitions)

        self._fill_optional_multi_source_anchors(
            split_anchors, optional_anchors, varied_keys, domain
        )
        return varied_keys

    def _collect_variations(
        self, y_definitions: list[dict[str, str]]
    ) -> tuple[list[str], list[str]]:
        varied_values = defaultdict(set)
        for defn in y_definitions:
            for key in FILENAME_FIELD:
                varied_values[key].add(defn.get(key, None))
            varied_values[CONCAT_FIELDS].add(
                FIELD_SEPARATOR.join([defn.get(FILENAME, ""), defn.get(FIELD, "")])
            )

        varied_keys = []

        for filename_or_field in FILENAME_FIELD:
            value_set = varied_values[filename_or_field]
            num_values = len(value_set)
            if num_values == 1:
                continue
            varied_keys.append(filename_or_field)

        domain = self._get_domain(varied_keys, varied_values)

        return varied_keys, domain

    def _fill_optional_multi_source_anchors(
        self,
        split_anchors: list[str],
        optional_anchors: list[str],
        varied_keys: list[str],
        domain: list[str],
    ):
        if not optional_anchors:
            return

        concat_field = FIELD_SEPARATOR.join(varied_keys)
        self._fill_group_by(split_anchors, optional_anchors, [REV, concat_field])

        self._fill_optional_anchor(
            split_anchors,
            optional_anchors,
            "pivot_field",
            f" + '{FIELD_SEPARATOR}' + ".join(
                [f"datum.{key}" for key in [REV, *varied_keys]]
            ),
        )

        self._fill_optional_anchor(
            split_anchors, optional_anchors, "row", {"field": concat_field, "sort": []}
        )
        self._fill_optional_anchor(
            split_anchors,
            optional_anchors,
            "column",
            {"field": concat_field, "sort": []},
        )

        self._fill_tooltip(split_anchors, optional_anchors, [concat_field])

        for anchor in ["stroke_dash", "shape"]:
            self._fill_optional_anchor_mapping(
                split_anchors, optional_anchors, concat_field, anchor, domain
            )

    def _fill_group_by(
        self,
        split_anchors: list[str],
        optional_anchors: list[str],
        group_by: list[str],
    ):
        self._fill_optional_anchor(
            split_anchors, optional_anchors, "group_by", group_by
        )
        self._fill_optional_anchor(
            split_anchors,
            optional_anchors,
            "group_by_x",
            [*group_by, self.properties.get("x")],
        )
        self._fill_optional_anchor(
            split_anchors,
            optional_anchors,
            "group_by_y",
            [*group_by, self.properties.get("y")],
        )

    def _fill_tooltip(
        self,
        split_anchors: list[str],
        optional_anchors: list[str],
        additional_fields: Optional[list[str]] = None,
    ):
        if not additional_fields:
            additional_fields = []
        self._fill_optional_anchor(
            split_anchors,
            optional_anchors,
            "tooltip",
            [
                {"field": REV},
                {"field": self.properties.get("x")},
                {"field": self.properties.get("y")},
                *[{"field": field} for field in additional_fields],
            ],
        )

    def _fill_optional_anchor(
        self,
        split_anchors: list[str],
        optional_anchors: list[str],
        name: str,
        value: Any,
    ):
        if name not in optional_anchors:
            return

        optional_anchors.remove(name)

        if name in split_anchors:
            self._set_split_content(name, value)
            return

        self.template.fill_anchor(name, value)

    def _get_domain(self, varied_keys: list[str], varied_values: dict[str, set]):
        if len(varied_keys) == 2:
            domain = list(varied_values[CONCAT_FIELDS])
        else:
            filename_or_field = varied_keys[0]
            domain = list(varied_values[filename_or_field])

        domain.sort()
        return domain

    def _fill_optional_anchor_mapping(  # noqa: PLR0913
        self,
        split_anchors: list[str],
        optional_anchors: list[str],
        field: str,
        name: str,
        domain: list[str],
    ):  # pylint: disable=too-many-arguments
        if name not in optional_anchors:
            return

        optional_anchors.remove(name)

        encoding = self._get_optional_anchor_mapping(field, name, domain)

        if name in split_anchors:
            self._set_split_content(name, encoding)
            return

        self.template.fill_anchor(name, encoding)

    def _get_optional_anchor_mapping(
        self,
        field: str,
        name: str,
        domain: list[str],
    ):
        full_range_values: list[Any] = OPTIONAL_ANCHOR_RANGES.get(name, [])
        anchor_range_values = full_range_values.copy()

        anchor_range = []
        for _ in range(len(domain)):
            if not anchor_range_values:
                anchor_range_values = full_range_values.copy()
            range_value = anchor_range_values.pop(0)
            anchor_range.append(range_value)

        legend = (
            # fix stroke dash and shape legend entry appearance (use empty shapes)
            {"legend": {"symbolFillColor": "transparent", "symbolStrokeColor": "grey"}}
            if name != "color"
            else {}
        )

        return {
            "field": field,
            "scale": {"domain": domain, "range": anchor_range},
            **legend,
        }

    def _update_datapoints(self, varied_keys: Optional[list[str]] = None):
        if varied_keys is None:
            return

        if len(varied_keys) == 2:
            to_concatenate = varied_keys
            to_remove = varied_keys
        else:
            to_concatenate = []
            to_remove = [key for key in FILENAME_FIELD if key not in varied_keys]

        for datapoint in self.datapoints:
            if to_concatenate:
                concat_key = FIELD_SEPARATOR.join(to_concatenate)
                datapoint[concat_key] = FIELD_SEPARATOR.join(
                    [datapoint.get(k) for k in to_concatenate]
                )
            for key in to_remove:
                datapoint.pop(key, None)

    def _set_split_content(self, name: str, value: Any):
        self._split_content[Template.anchor(name)] = value
