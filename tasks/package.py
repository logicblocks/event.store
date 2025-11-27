import os

from invoke import task, Context

@task
def publish(context: Context) -> None:
    """Publish library to pypi."""
    context.run("uv publish", env={
        "UV_PUBLISH_TOKEN": os.getenv("PYPI_API_KEY"),
    })
