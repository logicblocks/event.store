from collections.abc import Callable, Sequence

import griffe

from .code import Code

type Linker = Callable[[str], str | None]

MAXIMUM_LINE_LENGTH = 79


class SignatureRenderer:
    def __init__(self, link: Linker):
        self._link = link

    def expression(self, value: str | griffe.Expr | None) -> Code:
        if value is None:
            return Code()
        if isinstance(value, str):
            return Code.of(value)
        code = Code()
        for element in value.iterate(flat=True):
            if isinstance(element, griffe.ExprName):
                code = code + Code.of(
                    element.name, self._link(element.canonical_path)
                )
            else:
                code = code + str(element)
        return code

    def signature(self, target: griffe.Object, name: str) -> Code:
        match target:
            case griffe.Class():
                return self._class(target, name)
            case griffe.Function():
                return self._function(target, name)
            case griffe.TypeAlias():
                return self._type_alias(target, name)
            case griffe.Attribute():
                return self._attribute(target, name)
            case _:
                return Code.of(name)

    def _class(self, target: griffe.Class, name: str) -> Code:
        parameters = self._parameters(target.parameters, skip_first=True)
        head = "class " + name + self._type_parameters(target)
        return self._callable(head, parameters) if parameters else head

    def _function(self, target: griffe.Function, name: str) -> Code:
        if target.overloads:
            return Code.join(
                "\n",
                (
                    "@overload\n" + self._function(overload, name)
                    for overload in target.overloads
                ),
            )
        decorators = "".join(
            f"@{label}\n"
            for label in ("classmethod", "staticmethod")
            if label in target.labels
        )
        prefix = "async def " if "async" in target.labels else "def "
        skip_first = target.parent is not None and (
            target.parent.is_class and "staticmethod" not in target.labels
        )
        head = prefix + name + self._type_parameters(target)
        returns = self.expression(target.returns)
        tail = " -> " + returns if returns else Code()
        return decorators + self._callable(
            head, self._parameters(target.parameters, skip_first), tail
        )

    def _type_alias(self, target: griffe.TypeAlias, name: str) -> Code:
        return (
            "type "
            + name
            + self._type_parameters(target)
            + " = "
            + self.expression(target.value)
        )

    def _attribute(self, target: griffe.Attribute, name: str) -> Code:
        if "property" in target.labels:
            return "property " + self._annotated(name, target.annotation)
        code = self._annotated(name, target.annotation)
        if target.value is not None and not _is_instance_only(target):
            code = code + " = " + self.expression(target.value)
        return code

    def _annotated(
        self, name: str, annotation: str | griffe.Expr | None
    ) -> Code:
        rendered = self.expression(annotation)
        return name + (": " + rendered if rendered else Code())

    def _type_parameters(
        self,
        target: griffe.Class | griffe.Function | griffe.TypeAlias,
    ) -> Code:
        parameters = list(target.type_parameters)
        if not parameters:
            return Code()
        return (
            "["
            + Code.join(
                ", ",
                (self._type_parameter(parameter) for parameter in parameters),
            )
            + "]"
        )

    def _type_parameter(self, parameter: griffe.TypeParameter) -> Code:
        match parameter.kind:
            case griffe.TypeParameterKind.type_var_tuple:
                prefix = "*"
            case griffe.TypeParameterKind.param_spec:
                prefix = "**"
            case _:
                prefix = ""
        code = self._annotated(prefix + parameter.name, parameter.annotation)
        if parameter.default is not None:
            code = code + " = " + self.expression(parameter.default)
        return code

    def _parameters(
        self, parameters: griffe.Parameters, skip_first: bool
    ) -> list[Code]:
        listed = list(parameters)
        if skip_first and listed and listed[0].name in ("self", "cls"):
            listed = listed[1:]
        kinds = {parameter.kind for parameter in listed}
        rendered: list[Code] = []
        for index, parameter in enumerate(listed):
            if (
                parameter.kind is griffe.ParameterKind.keyword_only
                and griffe.ParameterKind.var_positional not in kinds
                and (
                    index == 0
                    or listed[index - 1].kind
                    is not griffe.ParameterKind.keyword_only
                )
            ):
                rendered.append(Code.of("*"))
            rendered.append(self._parameter(parameter))
            if parameter.kind is griffe.ParameterKind.positional_only and (
                index + 1 == len(listed)
                or listed[index + 1].kind
                is not griffe.ParameterKind.positional_only
            ):
                rendered.append(Code.of("/"))
        return rendered

    def _parameter(self, parameter: griffe.Parameter) -> Code:
        match parameter.kind:
            case griffe.ParameterKind.var_positional:
                prefix = "*"
            case griffe.ParameterKind.var_keyword:
                prefix = "**"
            case _:
                prefix = ""
        code = self._annotated(prefix + parameter.name, parameter.annotation)
        if parameter.default is not None and not prefix:
            separator = " = " if parameter.annotation is not None else "="
            code = code + separator + self.expression(parameter.default)
        return code

    @staticmethod
    def _callable(
        head: Code | str, parameters: Sequence[Code], tail: Code = Code()
    ) -> Code:
        single_line = head + "(" + Code.join(", ", parameters) + ")" + tail
        if len(single_line.text) <= MAXIMUM_LINE_LENGTH:
            return single_line
        lines = Code.join("", ("    " + p + ",\n" for p in parameters))
        return head + "(\n" + lines + ")" + tail


def _is_instance_only(target: griffe.Attribute) -> bool:
    return (
        "instance-attribute" in target.labels
        and "class-attribute" not in target.labels
    )
