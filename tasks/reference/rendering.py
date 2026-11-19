from collections.abc import Sequence

import griffe

from .docstrings import DocstringRenderer
from .links import ReferenceLinker
from .signatures import SignatureRenderer
from .surface import PublicModule, PublicObject

MAXIMUM_HEADING_LEVEL = 6


class PageRenderer:
    def __init__(self, linker: ReferenceLinker):
        self._signatures = SignatureRenderer(linker)
        self._docstrings = DocstringRenderer(
            self._signatures, linker.resolve_cross_references
        )

    def render(
        self, title: str, order: int, modules: Sequence[PublicModule]
    ) -> str:
        frontmatter = (
            "---\n"
            f"title: {title}\n"
            "sidebar:\n"
            f"  order: {order}\n"
            "editUrl: false\n"
            "---\n"
        )
        blocks = [frontmatter]
        for module in modules:
            blocks.extend(self._module_blocks(module))
        return "\n\n".join(block.rstrip("\n") for block in blocks) + "\n"

    def _module_blocks(self, module: PublicModule) -> list[str]:
        blocks = [_heading(2, module.path, module.path)]
        blocks.extend(self._docstrings.render(module.module))
        for public_object in module.objects:
            blocks.extend(self._object_blocks(public_object, 3))
        return blocks

    def _object_blocks(
        self, public_object: PublicObject, level: int
    ) -> list[str]:
        target = public_object.target
        signature = self._signatures.signature(
            target, public_object.name
        ).render()
        blocks = [
            _heading(level, public_object.path, public_object.name),
            f"<pre><code>{signature}</code></pre>",
        ]
        if isinstance(target, griffe.Class):
            blocks.extend(self._bases_blocks(target))
        blocks.extend(self._docstrings.render(target))
        for member in public_object.members:
            blocks.extend(
                self._object_blocks(
                    member, min(level + 1, MAXIMUM_HEADING_LEVEL)
                )
            )
        return blocks

    def _bases_blocks(self, target: griffe.Class) -> list[str]:
        if not target.bases:
            return []
        bases = ", ".join(
            self._signatures.expression(base).render_inline()
            for base in target.bases
        )
        return [f"**Bases:** {bases}"]


def _heading(level: int, anchor: str, text: str) -> str:
    return f"{'#' * level} `{text}` {{#{anchor}}}"
