import json
from typing import Any, Optional

from .base import Renderer
from .utils import list_dict_to_dict_list


class ParallelCoordinatesRenderer(Renderer):
    """
    Renderer for Parallel Coordinates plot.

    Using Plotly.
    """

    TYPE = "plotly"

    DIV = """
    <div id = "{id}">
        <script type = "text/javascript">
            var plotly_data = {partial};
            Plotly.newPlot("{id}", plotly_data.data, plotly_data.layout);
        </script>
    </div>
    """

    EXTENSIONS = {".json"}

    SCRIPTS = """
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    """

    # pylint: disable=W0231
    def __init__(
        self,
        datapoints,
        name="pcp",
        color_by: Optional[str] = None,
        fill_value: str = "",
    ):
        self.datapoints = datapoints
        self.color_by = color_by
        self.name = name
        self.fill_value = fill_value

    def partial_html(self, **kwargs) -> str:  # noqa: ARG002
        return json.dumps(self._get_plotly_data())

    def _get_plotly_data(self):
        tabular_dict = list_dict_to_dict_list(self.datapoints)

        trace: dict[str, Any] = {"type": "parcoords", "dimensions": []}
        for label, values in tabular_dict.items():
            values = list(map(str, values))
            is_categorical = False
            try:
                float_values = [
                    float(x) if x != self.fill_value else None for x in values
                ]
            except ValueError:
                is_categorical = True

            if is_categorical:
                non_missing = [x for x in values if x != self.fill_value]
                unique_values = sorted(set(non_missing))
                unique_values.append(self.fill_value)

                dummy_values = [unique_values.index(x) for x in values]

                values = [x if x != self.fill_value else "Missing" for x in values]
                trace["dimensions"].append(
                    {
                        "label": label,
                        "values": dummy_values,
                        "tickvals": dummy_values,
                        "ticktext": values,
                    }
                )
            else:
                trace["dimensions"].append({"label": label, "values": float_values})

            if label == self.color_by:
                trace["line"] = {
                    "color": dummy_values if is_categorical else float_values,
                    "showscale": True,
                    "colorbar": {"title": self.color_by},
                }
                if is_categorical:
                    trace["line"]["colorbar"]["tickmode"] = "array"
                    trace["line"]["colorbar"]["tickvals"] = dummy_values
                    trace["line"]["colorbar"]["ticktext"] = values

        return {"data": [trace], "layout": {}}
