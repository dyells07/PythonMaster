import abc
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from os import PathLike

StrPath = Union[str, "PathLike[str]"]


class Renderer(abc.ABC):
    """Base Renderer class"""

    DIV = """
    <div id="{id}">
      {partial}
    </div>
    """

    EXTENSIONS: Iterable[str] = {}

    def __init__(
        self,
        datapoints: Optional[list] = None,
        name: Optional[str] = None,
        **properties,
    ):
        self.datapoints = datapoints or []
        self.name = name or ""
        self.properties = properties

    @abc.abstractmethod
    def partial_html(self, **kwargs) -> str:
        """
        Us this method to generate HTML content,
        to fill `{partial}` inside self.DIV.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def TYPE(self):  # noqa: N802
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def SCRIPTS(self):  # noqa: N802
        raise NotImplementedError

    @staticmethod
    def remove_special_chars(string: str) -> str:
        "Ensure string is valid HTML id."
        return string.translate(
            {ord(c): "_" for c in r"!@#$%^&*()[]{};,<>?\/:.|`~=_+ "}
        )

    def generate_html(self, html_path=None) -> str:
        "Return `DIV` formatted with `partial_html`."
        partial = self.partial_html(html_path=html_path)
        if partial:
            div_id = self.remove_special_chars(self.name)

            return self.DIV.format(id=div_id, partial=partial)
        return ""

    def generate_markdown(self, report_path: Optional[StrPath] = None) -> str:  # pylint: disable=missing-function-docstring
        "Generate a markdown element"
        raise NotImplementedError

    @classmethod
    def matches(cls, filename, properties=None) -> bool:  # noqa: ARG003
        "Check if the Renderer is suitable."
        return Path(filename).suffix in cls.EXTENSIONS
