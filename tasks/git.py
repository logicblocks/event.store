from invoke import task, Context

from .shared import project


@task
def pull(context: Context):
    """Ensure current branch up to date with remote."""
    context.run("git pull")

@task
def set_author(context: Context):
    """Set the git author."""
    context.run("git config --global user.name 'InfraBlocks CI'")
    context.run("git config --global user.email 'ci@infrablocks.com'")

@task
def tag_version(context: Context):
    """Tag current git commit with current project version."""
    version = project.get_version(context)
    context.run(f"git tag -a 'v{version}' -m 'Release version {version}'")
    context.run(f"git push origin 'v{version}'")

@task
def push_version_bump(context: Context):
    """Push version bump commit to remote repository."""
    version = project.get_version(context)
    context.run("git add pyproject.toml")
    context.run(f"git commit -m 'Bump version to {version} [skip ci]'")
    context.run(f"git push origin HEAD")
