from pathlib import Path

from tasks.reference import generate_reference

FIXTURES = Path(__file__).parent / "fixtures"


def generate(output_directory: Path) -> list[str]:
    return list(
        generate_reference(
            package="acme.kit",
            search_paths=[FIXTURES],
            output_directory=output_directory,
            base_path="/kit",
            page_order=["surface", "signatures"],
        )
    )


def read_page(output_directory: Path, name: str) -> str:
    return (output_directory / f"{name}.md").read_text()


class TestPages:
    def test_writes_one_page_per_public_top_level_package(
        self, tmp_path: Path
    ):
        generate(tmp_path)

        assert sorted(path.name for path in tmp_path.iterdir()) == [
            "docstrings.md",
            "minimal.md",
            "references.md",
            "signatures.md",
            "surface.md",
        ]

    def test_omits_packages_with_nothing_to_document(self, tmp_path: Path):
        generate(tmp_path)

        assert not (tmp_path / "empty.md").exists()

    def test_removes_pages_from_previous_generations(self, tmp_path: Path):
        (tmp_path / "empty.md").write_text("stale")

        generate(tmp_path)

        assert not (tmp_path / "empty.md").exists()

    def test_writes_frontmatter_and_module_heading(self, tmp_path: Path):
        generate(tmp_path)

        assert read_page(tmp_path, "minimal") == (
            "---\n"
            "title: acme.kit.minimal\n"
            "sidebar:\n"
            "  order: 4\n"
            "editUrl: false\n"
            "---\n"
            "\n"
            "## `acme.kit.minimal` {#acme.kit.minimal}\n"
            "\n"
            "The minimal package.\n"
        )


class TestPublicSurface:
    def test_documents_exports_once_at_their_shallowest_public_module(
        self, tmp_path: Path
    ):
        generate(tmp_path)

        assert read_page(tmp_path, "surface") == (
            """---
title: acme.kit.surface
sidebar:
  order: 1
editUrl: false
---

## `acme.kit.surface` {#acme.kit.surface}

The surface package.

### `Gadget` {#acme.kit.surface.Gadget}

<pre><code>class Gadget</code></pre>

A gadget.

### `Widget` {#acme.kit.surface.Widget}

<pre><code>class Widget</code></pre>

A widget.

### `make_widget` {#acme.kit.surface.make_widget}

<pre><code>def make_widget()</code></pre>

Makes a widget.

## `acme.kit.surface.extras` {#acme.kit.surface.extras}

The extras package.

### `Doohickey` {#acme.kit.surface.extras.Doohickey}

<pre><code>class Doohickey</code></pre>

A doohickey.

## `acme.kit.surface.nested` {#acme.kit.surface.nested}

The nested package.

### `PartSettings` {#acme.kit.surface.nested.PartSettings}

<pre><code>class PartSettings</code></pre>

Part settings.

### `Sprocket` {#acme.kit.surface.nested.Sprocket}

<pre><code>class Sprocket</code></pre>

A sprocket.

## `acme.kit.surface.tools` {#acme.kit.surface.tools}

The tools module.

### `spanner` {#acme.kit.surface.tools.spanner}

<pre><code>def spanner() -&gt; None</code></pre>

A spanner.
"""
        )


class TestSignatures:
    def test_renders_escaped_signatures_without_docstrings(
        self, tmp_path: Path
    ):
        generate(tmp_path)

        assert read_page(tmp_path, "signatures") == (
            """---
title: acme.kit.signatures
sidebar:
  order: 2
editUrl: false
---

## `acme.kit.signatures` {#acme.kit.signatures}

### `Box` {#acme.kit.signatures.Box}

<pre><code>class Box[T: (int, str) = int](item: T)</code></pre>

#### `size` {#acme.kit.signatures.Box.size}

<pre><code>size: int</code></pre>

#### `LIMIT` {#acme.kit.signatures.Box.LIMIT}

<pre><code>LIMIT = 10</code></pre>

#### `item` {#acme.kit.signatures.Box.item}

<pre><code>item</code></pre>

#### `contents` {#acme.kit.signatures.Box.contents}

<pre><code>property contents: T</code></pre>

#### `empty` {#acme.kit.signatures.Box.empty}

<pre><code>@classmethod
def empty() -&gt; object</code></pre>

#### `kind` {#acme.kit.signatures.Box.kind}

<pre><code>@staticmethod
def kind() -&gt; str</code></pre>

#### `get` {#acme.kit.signatures.Box.get}

<pre><code>@overload
def get(index: int) -&gt; T
@overload
def get(index: slice) -&gt; list[T]</code></pre>

#### `__call__` {#acme.kit.signatures.Box.__call__}

<pre><code>def __call__() -&gt; T</code></pre>

### `Colour` {#acme.kit.signatures.Colour}

<pre><code>class Colour</code></pre>

**Bases:** <code>StrEnum</code>

#### `RED` {#acme.kit.signatures.Colour.RED}

<pre><code>RED = 'red'</code></pre>

### `DEFAULTS` {#acme.kit.signatures.DEFAULTS}

<pre><code>DEFAULTS: Mapping[str, int] = {'retries': 3}</code></pre>

### `Label` {#acme.kit.signatures.Label}

<pre><code>class Label</code></pre>

**Bases:** <code>str</code>

### `Pair` {#acme.kit.signatures.Pair}

<pre><code>type Pair[K, V] = tuple[K, V]</code></pre>

### `fetch` {#acme.kit.signatures.fetch}

<pre><code>async def fetch(*names: str, **options: int) -&gt; dict[str, list[int]]</code></pre>

### `first` {#acme.kit.signatures.first}

<pre><code>def first[T](
    items: Sequence[T],
    /,
    default: T | None = None,
    *,
    strict: bool = False,
) -&gt; T | None</code></pre>

### `register` {#acme.kit.signatures.register}

<pre><code>def register(callback: Callable[[str], None]) -&gt; None</code></pre>
"""
        )


class TestDocstrings:
    def test_renders_google_style_sections(self, tmp_path: Path):
        generate(tmp_path)

        assert read_page(tmp_path, "docstrings") == (
            """---
title: acme.kit.docstrings
sidebar:
  order: 3
editUrl: false
---

## `acme.kit.docstrings` {#acme.kit.docstrings}

The docstrings package.

It has a second paragraph.

### `Account` {#acme.kit.docstrings.Account}

<pre><code>class Account</code></pre>

A bank account.

**Attributes:**

- **`balance`** (<code>int</code>) — The current balance.
- **`owner`** (<code>str</code>) — The owner.

#### `balance` {#acme.kit.docstrings.Account.balance}

<pre><code>balance: int</code></pre>

#### `owner` {#acme.kit.docstrings.Account.owner}

<pre><code>owner: str</code></pre>

### `broken` {#acme.kit.docstrings.broken}

<pre><code>def broken(value: int) -&gt; None</code></pre>

Has a broken section.

### `transfer` {#acme.kit.docstrings.transfer}

<pre><code>def transfer(amount: int, currency: str = 'GBP') -&gt; bool</code></pre>

Transfers an amount.

The transfer is immediate.

**Parameters:**

- **`amount`** (<code>int</code>) — The amount to transfer.
- **`currency`** (<code>str</code>) — The currency, which may be
  spread over two lines.

**Returns:**

- <code>bool</code> — Whether the transfer succeeded.

**Raises:**

- <code>ValueError</code> — If the amount is negative.

:::note[Note]
Transfers are final.
:::

**Examples:**

```pycon
>>> transfer(10)
True
```
"""
        )


class TestWarnings:
    def test_returns_warnings_for_unparseable_docstrings(self, tmp_path: Path):
        docstrings_module = (
            (FIXTURES / "acme" / "kit" / "docstrings" / "__init__.py")
            .resolve()
            .relative_to(Path.cwd())
        )

        assert generate(tmp_path) == [
            f"{docstrings_module}:49: Failed to get 'name: description' "
            "pair from 'value without a colon'"
        ]


class TestReferences:
    def test_links_library_references_to_their_canonical_anchors(
        self, tmp_path: Path
    ):
        generate(tmp_path)

        report = '<a href="/kit/reference/references/#acme.kit.references.Report">Report</a>'
        widget = '<a href="/kit/reference/surface/#acme.kit.surface.Widget">Widget</a>'
        gadget = '<a href="/kit/reference/surface/#acme.kit.surface.Gadget">Gadget</a>'
        sprocket = '<a href="/kit/reference/surface/#acme.kit.surface.nested.Sprocket">Sprocket</a>'

        assert read_page(tmp_path, "references") == (
            f"""---
title: acme.kit.references
sidebar:
  order: 5
editUrl: false
---

## `acme.kit.references` {{#acme.kit.references}}

The references package.

See [`Widget`](/kit/reference/surface/#acme.kit.surface.Widget) and
[`Sprocket`](/kit/reference/surface/#acme.kit.surface.nested.Sprocket), but not
`Missing` or `empty`.

### `Report` {{#acme.kit.references.Report}}

<pre><code>class Report</code></pre>

A report.

Merge with [`merge`](/kit/reference/references/#acme.kit.references.Report.merge), or
use [acme.kit.surface.Widget](/kit/reference/surface/#acme.kit.surface.Widget).

#### `merge` {{#acme.kit.references.Report.merge}}

<pre><code>def merge(other: {report}) -&gt; {report}</code></pre>

### `summarise` {{#acme.kit.references.summarise}}

<pre><code>def summarise(
    widget: {widget},
    gadget: {gadget},
    sprocket: {sprocket},
    at: datetime,
    logger: structlog.BoundLogger,
) -&gt; {report}</code></pre>

Summarises.

**Parameters:**

- **`widget`** (<code>{widget}</code>) — The widget.

**Raises:**

- <code>LookupError</code> — If nothing is found.
"""
        )
