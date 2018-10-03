#!/usr/bin/env bash
yarn ci || FAIL=1

# sigh... mocha.
pushd ./workspaces/mytosis > /dev/null
yarn test || FAIL=1
popd > /dev/null

if [[ ! -z "$FAIL" ]]; then
  exit 1
fi
