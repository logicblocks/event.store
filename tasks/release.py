from invoke import Context, task

from . import package, git, version


@task
def prerelease(context: Context):
    """Release prerelease version of library."""
    git.set_author(context)
    package.publish(context)
    git.tag_version(context)
    version.bump_and_push(context, type="prerelease")


@task
def release(context: Context):
    """Release release version of library."""
    git.set_author(context)
    version.bump_and_push(context, type="patch")
    package.publish(context)
    git.tag_version(context)
    version.bump_and_push(context, type="prepatch")
