#!/usr/bin/env bash
# Pig 0.17.0 in MapReduce mode on Hadoop 3.3.6 (Java 8). Safe to re-run.
set -euo pipefail

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="$HOME/bigdata/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PIG_HOME="$HOME/bigdata/pig"
export PIG_CLASSPATH="$HADOOP_CONF_DIR"
export PATH="$PATH:$HADOOP_HOME/bin"
PIG_VERSION=0.17.0
HDFS="$HADOOP_HOME/bin/hdfs"

echo "== 1. Download Pig ${PIG_VERSION}"
# archive.apache.org throttles big files, so download from a mirror and verify against
# the checksum Apache publishes (only MD5 exists for this 2017 release).
TARBALL="pig-${PIG_VERSION}.tar.gz"
MIRROR="https://repo.huaweicloud.com/apache/pig/pig-${PIG_VERSION}/${TARBALL}"
CHECKSUM="https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/${TARBALL}.md5"
if [ ! -d "$PIG_HOME" ]; then
    cd "$HOME/bigdata"
    curl -fL --retry 3 -o "$TARBALL" "$MIRROR"
    curl -fsL "$CHECKSUM" | md5sum -c -
    tar -xzf "$TARBALL"
    mv "pig-${PIG_VERSION}" pig
    rm "$TARBALL"
fi

echo "== 2. Smoke test: word count in MapReduce mode"
SMOKE=/tmp/pig_smoke
"$HDFS" dfs -rm -r -f -skipTrash "$SMOKE" >/dev/null 2>&1 || true
"$HDFS" dfs -mkdir -p "$SMOKE"
printf 'hadoop pig hive\npig hive\npig\n' | "$HDFS" dfs -put - "$SMOKE/input.txt"
cat > /tmp/pig_smoke.pig <<'PIG'
lines  = LOAD '/tmp/pig_smoke/input.txt' AS (line:chararray);
words  = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;
groups = GROUP words BY word;
counts = FOREACH groups GENERATE group AS word, COUNT(words) AS n;
sorted = ORDER counts BY n DESC;
STORE sorted INTO '/tmp/pig_smoke/output';
PIG
"$PIG_HOME/bin/pig" -x mapreduce -l /tmp /tmp/pig_smoke.pig
echo "-- result (expect pig 3, hive 2, hadoop 1):"
"$HDFS" dfs -cat "$SMOKE/output/part-*"
echo "Pig ${PIG_VERSION} ready"
