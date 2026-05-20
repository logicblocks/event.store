from invoke import task, Context


@task
def install(context: Context):
    """Install all library dependencies."""
    context.run("uv sync --all-groups")

@task
def lock(context: Context):
    """Refreshes the uv lockfile."""
    context.run("uv lock")
