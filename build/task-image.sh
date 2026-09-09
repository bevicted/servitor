#!/usr/bin/env sh
set -eu

image=${1:?task image reference is required}
source=${ICT_SOURCE:-../ict}
docker=${DOCKER:-docker}
rsync=${RSYNC:-rsync}
platform=${IMAGE_PLATFORM:-linux/amd64}
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT HUP INT TERM

"$rsync" -a --exclude .git --exclude .terraform --exclude ict-workspaces ./ "$context/servitor-oss/"
"$rsync" -a --exclude .git --exclude .terraform --exclude ict-workspaces "$source/" "$context/ict/"
"$docker" build --platform "$platform" --file "$context/servitor-oss/build/task.Dockerfile" --tag "$image" "$context"
