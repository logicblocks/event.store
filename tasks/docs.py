from invoke import task, Context


@task
def serve(context: Context):
    context.run("mkdocs serve")

@task
def build(context: Context):
    context.run("mkdocs build")
