#!/usr/bin/env bash

[ -n "$DEBUG" ] && set -x
set -e
set -o pipefail

apt-get --allow-releaseinfo-change update
apt-get install -y --no-install-recommends \
  git \
  ssh \
  git-crypt
