from invoke import Context, task

from . import package, git, version, build


@task
def prerelease(context: Context):
    """Release prerelease version of library."""
    git.set_author(context)
    build.build(context)
    package.publish(context)
    git.tag_version(context)
    version.bump_and_push(context, type=["alpha"])


@task
def release(context: Context):
    """Release release version of library."""
    git.set_author(context)
    version.bump_and_push(context, type=["patch"])
    build.build(context)
    package.publish(context)
    git.tag_version(context)
    version.bump_and_push(context, type="prepatch")
