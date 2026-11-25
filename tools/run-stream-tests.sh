#!/bin/sh
set -eu
: "${APELIN_PROJECT_ROOT:?Run from an Apelin project terminal}"
cd "${APELIN_PROJECT_ROOT:?}/tools"
clang -arch arm64e -fobjc-arc -O1 -Wall -Wextra \
    -Wno-unused-function -Wno-unused-variable -Wno-unused-parameter \
    StreamTimeoutTests.m -framework Foundation -framework UIKit -o .stream-tests
ldid -STool.entitlements .stream-tests
exec ./.stream-tests
