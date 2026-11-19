from collections.abc import Callable, Sequence

import griffe

from .signatures import SignatureRenderer


class DocstringRenderer:
    def __init__(
        self,
        signatures: SignatureRenderer,
        resolve_cross_references: Callable[[str], str],
    ):
        self._signatures = signatures
        self._resolve = resolve_cross_references

    def render(self, target: griffe.Object) -> list[str]:
        if target.docstring is None:
            return []
        blocks: list[str] = []
        for section in target.docstring.parsed:
            blocks.extend(self._section(section))
        return blocks

    def _section(self, section: griffe.DocstringSection) -> list[str]:
        match section:
            case griffe.DocstringSectionText():
                return [self._resolve(section.value)]
            case griffe.DocstringSectionAdmonition():
                admonition = section.value
                title = section.title or str(admonition.annotation).title()
                description = self._resolve(admonition.description)
                return [f":::note[{title}]\n{description}\n:::"]
            case griffe.DocstringSectionExamples():
                return ["**Examples:**", *self._examples(section.value)]
            case _:
                return self._items(section)

    def _items(self, section: griffe.DocstringSection) -> list[str]:
        items = [self._item(element) for element in section.value]
        if not items:
            return []
        title = section.kind.value.replace("-", " ").capitalize()
        return [f"**{title}:**", "\n".join(items)]

    def _item(self, element: griffe.DocstringElement) -> str:
        parts = []
        if isinstance(element, griffe.DocstringNamedElement) and (
            element.name
        ):
            parts.append(f"**`{element.name}`**")
        annotation = self._signatures.expression(element.annotation)
        if annotation:
            rendered = annotation.render_inline()
            parts.append(f"({rendered})" if parts else rendered)
        description = self._resolve(element.description).replace("\n", "\n  ")
        return "- " + " — ".join(
            part for part in (" ".join(parts), description) if part
        )

    @staticmethod
    def _examples(
        examples: Sequence[tuple[griffe.DocstringSectionKind, str]],
    ) -> list[str]:
        return [
            f"```pycon\n{text}\n```"
            if kind is griffe.DocstringSectionKind.examples
            else text
            for kind, text in examples
        ]
