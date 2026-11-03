from collections.abc import Sequence
from pathlib import Path

import griffe

from .links import ReferenceLinker
from .rendering import PageRenderer
from .surface import PublicSurface
from .warnings import collected_griffe_warnings


def generate_reference(
    package: str,
    search_paths: Sequence[Path],
    output_directory: Path,
    base_path: str,
    page_order: Sequence[str] = (),
) -> Sequence[str]:
    with collected_griffe_warnings() as warnings:
        root = griffe.load(
            package,
            search_paths=[str(path) for path in search_paths],
            docstring_parser="google",
        )
        if not isinstance(root, griffe.Module):
            raise ValueError(f"{package} is not a module")
        surface = PublicSurface(root)
        names = _ordered(surface.package_names(), page_order)
        output_directory.mkdir(parents=True, exist_ok=True)
        renderer = PageRenderer(ReferenceLinker(surface, base_path))
        for order, name in enumerate(names, start=1):
            page = renderer.render(
                f"{root.path}.{name}", order, surface.modules_in(name)
            )
            (output_directory / f"{name}.md").write_text(page)
    return warnings


def _ordered(names: Sequence[str], preferred: Sequence[str]) -> list[str]:
    first = [name for name in preferred if name in names]
    return first + [name for name in names if name not in first]
