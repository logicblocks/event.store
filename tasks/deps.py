from invoke import task, Context


@task
def install(context: Context):
    """Install all library dependencies."""
    context.run("poetry install")
