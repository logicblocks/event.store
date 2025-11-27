from invoke import task, Context


@task
def create_fragment(context: Context):
    context.run("scriv create")

@task
def assemble(context: Context):
    context.run("scriv collect")
