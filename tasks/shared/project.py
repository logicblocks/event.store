from invoke import Context


def get_version(context: Context):
    return context.run("uv version --short --no-color").stdout.strip()
