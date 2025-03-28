#!/usr/bin/env bash

[ -n "$DEBUG" ] && set -x
set -e
set -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/../../.." && pwd )"

cd "$PROJECT_DIR"

git crypt unlock

./go poetry:login_to_pypi
./go library:publish:prerelease

VERSION=$(poetry version | cut -d' ' -f2)

git commit -a -m "Bump version to $VERSION for prerelease [ci skip]"
git push
