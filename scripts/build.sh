#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

rm -rf build/classes build/linkmesh.jar
mkdir -p build/classes
find src/main/java -name '*.java' -print0 | xargs -0 javac --release 21 -d build/classes
jar --create --file build/linkmesh.jar --main-class linkmesh.Main -C build/classes .

echo "Built build/linkmesh.jar ($(find src/main/java -name '*.java' | wc -l | tr -d ' ') source files)"
echo "Try: java -jar build/linkmesh.jar help"
