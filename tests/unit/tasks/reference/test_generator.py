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
            '<h2 id="acme.kit.minimal"><code>acme.kit.minimal</code></h2>\n'
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

<h2 id="acme.kit.surface"><code>acme.kit.surface</code></h2>

The surface package.

<h3 id="acme.kit.surface.Gadget"><code>Gadget</code></h3>

<pre><code>class Gadget</code></pre>

A gadget.

<h3 id="acme.kit.surface.Widget"><code>Widget</code></h3>

<pre><code>class Widget</code></pre>

A widget.

<h3 id="acme.kit.surface.make_widget"><code>make_widget</code></h3>

<pre><code>def make_widget()</code></pre>

Makes a widget.

<h2 id="acme.kit.surface.extras"><code>acme.kit.surface.extras</code></h2>

The extras package.

<h3 id="acme.kit.surface.extras.Doohickey"><code>Doohickey</code></h3>

<pre><code>class Doohickey</code></pre>

A doohickey.

<h2 id="acme.kit.surface.nested"><code>acme.kit.surface.nested</code></h2>

The nested package.

<h3 id="acme.kit.surface.nested.PartSettings"><code>PartSettings</code></h3>

<pre><code>class Settings</code></pre>

Part settings.

<h3 id="acme.kit.surface.nested.Sprocket"><code>Sprocket</code></h3>

<pre><code>class Sprocket</code></pre>

A sprocket.

<h2 id="acme.kit.surface.tools"><code>acme.kit.surface.tools</code></h2>

The tools module.

<h3 id="acme.kit.surface.tools.spanner"><code>spanner</code></h3>

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

<h2 id="acme.kit.signatures"><code>acme.kit.signatures</code></h2>

<h3 id="acme.kit.signatures.Box"><code>Box</code></h3>

<pre><code>class Box[T: (int, str) = int](item: T)</code></pre>

<h4 id="acme.kit.signatures.Box.size"><code>size</code></h4>

<pre><code>size: int</code></pre>

<h4 id="acme.kit.signatures.Box.item"><code>item</code></h4>

<pre><code>item</code></pre>

<h4 id="acme.kit.signatures.Box.contents"><code>contents</code></h4>

<pre><code>property contents: T</code></pre>

<h4 id="acme.kit.signatures.Box.empty"><code>empty</code></h4>

<pre><code>@classmethod
def empty() -&gt; object</code></pre>

<h4 id="acme.kit.signatures.Box.kind"><code>kind</code></h4>

<pre><code>@staticmethod
def kind() -&gt; str</code></pre>

<h4 id="acme.kit.signatures.Box.get"><code>get</code></h4>

<pre><code>@overload
def get(index: int) -&gt; T
@overload
def get(index: slice) -&gt; list[T]</code></pre>

<h3 id="acme.kit.signatures.DEFAULTS"><code>DEFAULTS</code></h3>

<pre><code>DEFAULTS: Mapping[str, int] = {'retries': 3}</code></pre>

<h3 id="acme.kit.signatures.Label"><code>Label</code></h3>

<pre><code>class Label</code></pre>

**Bases:** <code>str</code>

<h3 id="acme.kit.signatures.Pair"><code>Pair</code></h3>

<pre><code>type Pair[K, V] = tuple[K, V]</code></pre>

<h3 id="acme.kit.signatures.fetch"><code>fetch</code></h3>

<pre><code>async def fetch(*names: str, **options: int) -&gt; dict[str, list[int]]</code></pre>

<h3 id="acme.kit.signatures.first"><code>first</code></h3>

<pre><code>def first[T](
    items: Sequence[T],
    /,
    default: T | None = None,
    *,
    strict: bool = False,
) -&gt; T | None</code></pre>

<h3 id="acme.kit.signatures.register"><code>register</code></h3>

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

<h2 id="acme.kit.docstrings"><code>acme.kit.docstrings</code></h2>

The docstrings package.

It has a second paragraph.

<h3 id="acme.kit.docstrings.Account"><code>Account</code></h3>

<pre><code>class Account</code></pre>

A bank account.

**Attributes:**

- **`balance`** (<code>int</code>) — The current balance.
- **`owner`** (<code>str</code>) — The owner.

<h4 id="acme.kit.docstrings.Account.balance"><code>balance</code></h4>

<pre><code>balance: int</code></pre>

<h4 id="acme.kit.docstrings.Account.owner"><code>owner</code></h4>

<pre><code>owner: str</code></pre>

<h3 id="acme.kit.docstrings.broken"><code>broken</code></h3>

<pre><code>def broken(value: int) -&gt; None</code></pre>

Has a broken section.

<h3 id="acme.kit.docstrings.transfer"><code>transfer</code></h3>

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

<h2 id="acme.kit.references"><code>acme.kit.references</code></h2>

The references package.

See [`Widget`](/kit/reference/surface/#acme.kit.surface.Widget) and
[`Sprocket`](/kit/reference/surface/#acme.kit.surface.nested.Sprocket), but not
`Missing`.

<h3 id="acme.kit.references.Report"><code>Report</code></h3>

<pre><code>class Report</code></pre>

A report.

Merge with [`merge`](/kit/reference/references/#acme.kit.references.Report.merge), or
use [acme.kit.surface.Widget](/kit/reference/surface/#acme.kit.surface.Widget).

<h4 id="acme.kit.references.Report.merge"><code>merge</code></h4>

<pre><code>def merge(other: {report}) -&gt; {report}</code></pre>

<h3 id="acme.kit.references.summarise"><code>summarise</code></h3>

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
