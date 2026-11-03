from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass

import griffe


@dataclass(frozen=True)
class PublicObject:
    path: str
    target: griffe.Object
    members: Sequence["PublicObject"] = ()

    @property
    def name(self) -> str:
        return self.path.rsplit(".", 1)[-1]

    def flattened(self) -> Iterator["PublicObject"]:
        yield self
        for member in self.members:
            yield from member.flattened()


@dataclass(frozen=True)
class PublicModule:
    module: griffe.Module
    objects: Sequence[PublicObject]

    @property
    def path(self) -> str:
        return self.module.path


class PublicSurface:
    def __init__(self, root: griffe.Module):
        self._root = root
        modules = list(_public_modules(root))
        self._canonical_paths = _canonical_paths(modules)
        candidates = (
            PublicModule(module, self._objects_in(module))
            for module in sorted(modules, key=lambda m: m.path)
        )
        self._modules = [
            candidate
            for candidate in candidates
            if _is_presented(candidate, root)
        ]
        self._public_paths = _public_paths(self._modules)

    @property
    def root_path(self) -> str:
        return self._root.path

    def package_names(self) -> list[str]:
        return sorted(
            name for name in self._root.modules if not name.startswith("_")
        )

    def modules_in(self, package_name: str) -> list[PublicModule]:
        package_path = f"{self._root.path}.{package_name}"
        return [
            module
            for module in self._modules
            if module.path == package_path
            or module.path.startswith(package_path + ".")
        ]

    def public_path_of(self, path: str) -> str | None:
        if path in self._public_paths:
            return self._public_paths[path]
        if not path.startswith(self._root.path + "."):
            return None
        try:
            member = self._root.modules_collection[path]
        except (KeyError, ValueError, griffe.AliasResolutionError):
            return None
        target = _final_target(member)
        if target is None:
            return None
        return self._public_paths.get(target.path)

    def _objects_in(self, module: griffe.Module) -> list[PublicObject]:
        return [
            _public_object(path, target)
            for name, target in _exports(module)
            if not isinstance(target, griffe.Module)
            and self._canonical_paths[target.path]
            == (path := f"{module.path}.{name}")
        ]


def _public_object(path: str, target: griffe.Object) -> PublicObject:
    members = (
        [
            _public_object(f"{path}.{name}", member)
            for name, member in target.members.items()
            if not name.startswith("_")
            and not isinstance(member, griffe.Alias)
        ]
        if isinstance(target, griffe.Class)
        else []
    )
    return PublicObject(path, target, members)


def _is_presented(module: PublicModule, root: griffe.Module) -> bool:
    is_package = module.module.parent is root
    return is_package or bool(module.objects) or bool(module.module.docstring)


def _public_paths(modules: Sequence[PublicModule]) -> Mapping[str, str]:
    paths: dict[str, str] = {}
    for module in modules:
        paths[module.path] = module.path
        for public_object in module.objects:
            for documented in public_object.flattened():
                paths[documented.target.path] = documented.path
                paths[documented.path] = documented.path
    return paths


def _public_modules(root: griffe.Module) -> Iterator[griffe.Module]:
    visited: set[str] = set()

    def walk(module: griffe.Module) -> Iterator[griffe.Module]:
        if module.path in visited:
            return
        visited.add(module.path)
        yield module
        for _, target in _exports(module):
            if isinstance(target, griffe.Module):
                yield from walk(target)
        for name, submodule in module.modules.items():
            if not name.startswith("_") and submodule.exports is not None:
                yield from walk(submodule)

    for name, package in root.modules.items():
        if not name.startswith("_"):
            yield from walk(package)


def _exports(
    module: griffe.Module,
) -> Iterator[tuple[str, griffe.Object]]:
    for name in _exported_names(module):
        if name.startswith("_") or name not in module.members:
            continue
        target = _final_target(module.members[name])
        if target is not None:
            yield name, target


def _exported_names(module: griffe.Module) -> list[str]:
    if module.exports is not None:
        return [str(export) for export in module.exports]
    return [
        name
        for name, member in module.members.items()
        if not isinstance(member, (griffe.Alias, griffe.Module))
    ]


def _final_target(
    member: griffe.Object | griffe.Alias,
) -> griffe.Object | None:
    if not isinstance(member, griffe.Alias):
        return member
    try:
        return member.final_target
    except (griffe.AliasResolutionError, griffe.CyclicAliasError):
        return None


def _canonical_paths(
    modules: Sequence[griffe.Module],
) -> Mapping[str, str]:
    candidates: defaultdict[str, list[tuple[griffe.Module, str]]] = (
        defaultdict(list)
    )
    for module in modules:
        for name, target in _exports(module):
            if not isinstance(target, griffe.Module):
                candidates[target.path].append(
                    (module, f"{module.path}.{name}")
                )

    def preference(
        target_path: str, candidate: tuple[griffe.Module, str]
    ) -> tuple[int, bool, bool, str]:
        module, path = candidate
        return (
            module.path.count("."),
            not target_path.startswith(module.path + "."),
            path.rsplit(".", 1)[-1] != target_path.rsplit(".", 1)[-1],
            path,
        )

    return {
        target_path: min(
            options, key=lambda option: preference(target_path, option)
        )[1]
        for target_path, options in candidates.items()
    }
