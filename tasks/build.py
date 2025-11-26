from invoke import task, Context


@task
def build(context: Context):
    """Build distribution packages."""
    context.run("poetry build")
