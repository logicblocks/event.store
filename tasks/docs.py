from invoke import task, Context


@task
def serve(context: Context):
    with context.cd("docs"):
        context.run("npm ci")
        context.run("npm run dev", pty=True)


@task
def build(context: Context):
    with context.cd("docs"):
        context.run("npm ci")
        context.run("npm run build")
