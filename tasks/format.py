from invoke import task, Context


@task
def check(context: Context):
    """Run formatter and check for errors."""
    context.run("ruff format --diff src tests")

@task
def fix(context: Context):
    """Run formatter and fix errors automatically."""
    context.run("ruff format src tests")
