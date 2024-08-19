from pathlib import Path
from typing import TYPE_CHECKING, Optional

from .exceptions import MissingPlaceholderError

if TYPE_CHECKING:
    from .base import Renderer, StrPath


PAGE_MARKDOWN = """# DVC Report
{renderers}
"""


class Markdown:
    RENDERERS_PLACEHOLDER = "renderers"
    RENDERERS_PLACEHOLDER_FORMAT_STR = f"{{{RENDERERS_PLACEHOLDER}}}"

    def __init__(
        self,
        template: Optional[str] = None,
    ):
        template = template or PAGE_MARKDOWN
        if self.RENDERERS_PLACEHOLDER_FORMAT_STR not in template:
            raise MissingPlaceholderError(
                self.RENDERERS_PLACEHOLDER_FORMAT_STR, "Markdown"
            )

        self.template = template
        self.elements: list[str] = []

    def with_element(self, md: str) -> "Markdown":
        "Adds custom markdown element."
        self.elements.append(md)
        return self

    def embed(self) -> str:
        "Format Markdown template with all elements."
        kwargs = {
            self.RENDERERS_PLACEHOLDER: "\n".join(self.elements),
        }
        for placeholder, value in kwargs.items():
            self.template = self.template.replace("{" + placeholder + "}", value)
        return self.template


def render_markdown(
    renderers: list["Renderer"],
    output_file: Optional["StrPath"] = None,
    template_path: Optional["StrPath"] = None,
) -> "StrPath":
    "User renderers to fill an Markdown template and write to path."
    output_path = None
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(exist_ok=True)

    page = None
    if template_path:
        with open(template_path, encoding="utf-8") as fobj:
            page = fobj.read()

    document = Markdown(page)

    for renderer in renderers:
        document.with_element(renderer.generate_markdown(report_path=output_path))

    if output_file and output_path:
        output_path.write_text(document.embed(), encoding="utf8")

        return output_file

    return document.embed()
