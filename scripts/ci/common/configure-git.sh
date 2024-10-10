#!/usr/bin/env bash

[ -n "$DEBUG" ] && set -x
set -e
set -o pipefail

#git config --global user.email "info@go-atomic.io"
#git config --global user.name "Circle CI"

gpg --list-secret-keys
git crypt unlock