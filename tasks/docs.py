from pathlib import Path

from invoke import Context, task

REFERENCE_DIRECTORY = Path("docs/src/content/docs/reference")
PACKAGE_ORDER = [
    "store",
    "types",
    "projection",
    "processing",
    "persistence",
    "query",
    "sources",
    "testing",
    "utils",
]


@task
def reference(context: Context):
    """Generates the API reference pages from the source."""
    # Deferred so other tasks run before the docs group is installed.
    from .reference import generate_reference

    warnings = generate_reference(
        package="logicblocks.event",
        search_paths=[Path("src")],
        output_directory=REFERENCE_DIRECTORY,
        base_path="/event.store",
        page_order=PACKAGE_ORDER,
    )
    for warning in warnings:
        print(f"WARNING: {warning}")


@task(pre=[reference])
def serve(context: Context):
    with context.cd("docs"):
        context.run("npm ci")
        context.run("npm run dev", pty=True)


@task(pre=[reference])
def build(context: Context):
    with context.cd("docs"):
        context.run("npm ci")
        context.run("npm run build")
