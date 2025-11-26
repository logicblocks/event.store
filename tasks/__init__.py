from invoke import Collection

from . import (
    build,
    deps,
    format,
    git,
    lint,
    package,
    release,
    test,
    types,
    version
)

ns = Collection()

ns.add_task(build.build, name="build")
ns.add_task(release.prerelease, name="prerelease")
ns.add_task(release.release, name="release")

ns.add_collection(Collection.from_module(deps))
ns.add_collection(Collection.from_module(format))
ns.add_collection(Collection.from_module(git))
ns.add_collection(Collection.from_module(lint))
ns.add_collection(Collection.from_module(package))
ns.add_collection(Collection.from_module(test))
ns.add_collection(Collection.from_module(types))
ns.add_collection(Collection.from_module(version))
