import os

from invoke import task, Context

@task
def publish(context: Context) -> None:
    """Publish library to pypi."""
    context.run(f"poetry publish --build", env={
        "POETRY_PYPI_TOKEN_PYPI": os.getenv("PYPI_API_KEY"),
    })
