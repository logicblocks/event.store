from invoke import task, Context


@task
def check(context: Context):
    """Run type checker and check for errors."""
    context.run("pyright src tests")
