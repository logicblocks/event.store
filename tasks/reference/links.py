import re

from .surface import PublicSurface

_CROSS_REFERENCE = re.compile(
    r"\[(?P<text>[^\]\n]+)\]\[(?P<identifier>[^\]\s]*)\]"
)


class ReferenceLinker:
    def __init__(self, surface: PublicSurface, base_path: str):
        self._surface = surface
        self._base_path = base_path.rstrip("/")

    def __call__(self, path: str) -> str | None:
        public_path = self._surface.public_path_of(path)
        if public_path is None:
            return None
        relative_path = public_path.removeprefix(self._surface.root_path + ".")
        package = relative_path.split(".")[0]
        return f"{self._base_path}/reference/{package}/#{public_path}"

    def resolve_cross_references(self, markdown: str) -> str:
        def replace(match: re.Match[str]) -> str:
            text = match["text"]
            identifier = match["identifier"] or text.strip("`")
            href = self(identifier)
            return f"[{text}]({href})" if href else text

        return _CROSS_REFERENCE.sub(replace, markdown)
