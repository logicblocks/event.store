import html
from collections.abc import Callable, Iterable
from dataclasses import dataclass

type Token = tuple[str, str | None]

_MARKDOWN_ENTITIES = str.maketrans(
    {
        "*": "&#42;",
        "_": "&#95;",
        "[": "&#91;",
        "]": "&#93;",
        "`": "&#96;",
        "\\": "&#92;",
        "|": "&#124;",
    }
)


def escape_block(text: str) -> str:
    return html.escape(text, quote=False)


def escape_inline(text: str) -> str:
    return escape_block(text).translate(_MARKDOWN_ENTITIES)


@dataclass(frozen=True)
class Code:
    tokens: tuple[Token, ...] = ()

    @classmethod
    def of(cls, text: str, href: str | None = None) -> "Code":
        return cls(((text, href),)) if text else cls()

    @classmethod
    def join(cls, separator: str, parts: Iterable["Code"]) -> "Code":
        joined = cls()
        for index, part in enumerate(parts):
            joined = joined + separator + part if index else part
        return joined

    @property
    def text(self) -> str:
        return "".join(text for text, _ in self.tokens)

    def __bool__(self) -> bool:
        return bool(self.tokens)

    def __add__(self, other: "Code | str") -> "Code":
        other_code = Code.of(other) if isinstance(other, str) else other
        return Code(self.tokens + other_code.tokens)

    def __radd__(self, other: str) -> "Code":
        return Code.of(other) + self

    def render(self, escape: Callable[[str], str] = escape_block) -> str:
        return "".join(
            f'<a href="{html.escape(href)}">{escape(text)}</a>'
            if href
            else escape(text)
            for text, href in self.tokens
        )

    def render_inline(self) -> str:
        return f"<code>{self.render(escape_inline)}</code>"
