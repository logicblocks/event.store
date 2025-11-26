from invoke import task, Context


@task
def check(context: Context):
    """Run linter and check for errors."""
    context.run("ruff check src tests")

@task
def fix(context: Context):
    """Run linter and fix errors automatically."""
    context.run("ruff check --fix src tests")
