from invoke import Context


def get_version(context: Context):
    return context.run("poetry version --short").stdout.strip()
