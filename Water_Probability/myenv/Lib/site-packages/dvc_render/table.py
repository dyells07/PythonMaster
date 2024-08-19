from .base import Renderer
from .utils import list_dict_to_dict_list

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None


class TableRenderer(Renderer):
    """Renderer for tables."""

    TYPE = "table"
    DIV = """
        <div id="{id}" style="text-align: center; padding: 10x">
            <p>{id}</p>
            <div style="display: flex;justify-content: center;">
                {partial}
            </div>
        </div>"""

    SCRIPTS = ""

    EXTENSIONS = {".yml", ".yaml", ".json"}

    @classmethod
    def to_tabulate(cls, datapoints, tablefmt):
        """Convert datapoints to tabulate format"""
        if tabulate is None:
            raise ImportError(f"{cls.__name__} requires `tabulate`.")  # noqa: TRY003
        data = list_dict_to_dict_list(datapoints)
        return tabulate(data, headers="keys", tablefmt=tablefmt)

    def partial_html(self, **kwargs) -> str:  # noqa: ARG002
        return self.to_tabulate(self.datapoints, tablefmt="html")

    def generate_markdown(self, report_path=None) -> str:  # noqa: ARG002
        table = self.to_tabulate(self.datapoints, tablefmt="github")
        return f"\n{self.name}\n\n{table}"
