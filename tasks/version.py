from invoke import task, Context

from . import git


@task
def bump(context: Context, type: str = "prerelease"):
    """Bump project version."""
    context.run(f"poetry version {type}")

@task
def bump_and_push(context: Context, type: str = "prerelease"):
    """Bump project version and push to remote repository."""
    git.pull(context)
    bump(context, type=type)
    git.push_version_bump(context)
