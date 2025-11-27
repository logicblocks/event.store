from collections.abc import Sequence

from invoke import task, Context

from . import git


type_help_description = (
    'The type of version bump to perform. '
    'Can be provided more than once for more specific version bump '
    'behaviour (e.g., patch and alpha moves to the next prerelease '
    'patch version). Supported types: '
    'major, minor, patch, stable, alpha, beta, rc, post, dev'
)


@task(iterable=['type'], help={'type': type_help_description})
def bump(context: Context, type: Sequence[str] = ("alpha",)):
    """Bump project version."""
    bump_list = [t.strip() for t in type]
    bump_args = " ".join(f"--bump {t}" for t in bump_list)
    context.run(f"uv version {bump_args}")

@task(iterable=['type'], help={'type': type_help_description})
def bump_and_push(context: Context, type: Sequence[str] = ("alpha",)):
    """Bump project version and push to remote repository."""
    git.pull(context)
    bump(context, type=type)
    git.push_version_bump(context)
