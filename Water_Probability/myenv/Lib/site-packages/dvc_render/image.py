import os

from .base import Renderer


class ImageRenderer(Renderer):
    """Renderer for image plots."""

    TYPE = "image"
    DIV = """
        <div
            id="{id}"
            style="border:1px solid black;text-align:center;
            white-space: nowrap;overflow-y:hidden;">
            {partial}
        </div>"""

    TITLE_FIELD = "rev"
    SRC_FIELD = "src"

    SCRIPTS = ""

    EXTENSIONS = {".jpg", ".jpeg", ".gif", ".png", ".svg"}

    def partial_html(self, html_path=None, **kwargs) -> str:  # noqa: ARG002
        div_content = []
        for datapoint in self.datapoints:
            src = datapoint[self.SRC_FIELD]

            if (
                not src.startswith("data:image;base64")
                and os.path.isabs(src)
                and html_path
            ):
                src = os.path.relpath(src, os.path.dirname(html_path))

            div_content.append(
                f"""
                <div
                    style="border:1px dotted black;margin:2px;display:
                    inline-block;
                    overflow:hidden;margin-left:8px;">
                    <p>{datapoint[self.TITLE_FIELD]}</p>
                    <img src="{src}">
                </div>
                """
            )
        if div_content:
            div_content.insert(0, f"<p>{self.name}</p>")
            return "\n".join(div_content)
        return ""

    def generate_markdown(self, report_path=None) -> str:  # noqa: ARG002
        content = []
        for datapoint in self.datapoints:
            src = datapoint[self.SRC_FIELD]
            if src.startswith("data:image;base64"):
                src = src.replace("data:image;base64", "data:image/png;base64")
            content.append(f"\n![{datapoint[self.TITLE_FIELD]}]({src})")
        if content:
            return "\n".join(content)
        return ""
