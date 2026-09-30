#!/usr/bin/env bash
# Compile the Java MapReduce jobs into mapreduce/gitlytics-mr.jar (Java 8, Hadoop classpath).
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/scripts/env.sh"
cd "$GITLYTICS_HOME/mapreduce"

rm -rf build && mkdir -p build
find . -path ./build -prune -o -name "*.java" -print > build/sources.txt
javac -source 8 -target 8 -Xlint:-options -classpath "$(hadoop classpath)" -d build @build/sources.txt
jar cf gitlytics-mr.jar -C build .
echo "Built $(pwd)/gitlytics-mr.jar ($(wc -l < build/sources.txt) source files)"
