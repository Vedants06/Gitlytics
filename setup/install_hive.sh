#!/usr/bin/env bash
# Hive 3.1.3 on Hadoop 3.3.6 (Java 8), MapReduce engine.
# Metastore: database "metastore" in the gitlytics-airflow-db Postgres container (host port 5433),
# so several Hive sessions (CLI, Airflow, beeline) can run at once. Safe to re-run.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
set -a; . "$REPO/.env"; set +a

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
HADOOP_HOME="$HOME/bigdata/hadoop"
HIVE_HOME="$HOME/bigdata/hive"
HIVE_VERSION=3.1.3
PG_JDBC=postgresql-42.7.4.jar
HDFS="$HADOOP_HOME/bin/hdfs"

echo "== 1. Download Hive ${HIVE_VERSION}"
# archive.apache.org throttles big files (~100 KB/s), so download from a mirror and
# verify against the SHA-256 that Apache publishes on archive.apache.org.
TARBALL="apache-hive-${HIVE_VERSION}-bin.tar.gz"
MIRROR="https://repo.huaweicloud.com/apache/hive/hive-${HIVE_VERSION}/${TARBALL}"
CHECKSUM="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${TARBALL}.sha256"
if [ ! -d "$HIVE_HOME" ]; then
    cd "$HOME/bigdata"
    curl -fL --retry 3 -o "$TARBALL" "$MIRROR"
    curl -fsL "$CHECKSUM" | sha256sum -c -
    tar -xzf "$TARBALL"
    mv "apache-hive-${HIVE_VERSION}-bin" hive
    rm "$TARBALL"
fi

echo "== 2. Jar fixes"
# Hive ships guava 19; Hadoop 3.3.6 needs 27. Without this Hive fails with NoSuchMethodError.
rm -f "$HIVE_HOME/lib/guava-19.0.jar"
cp "$HADOOP_HOME/share/hadoop/common/lib/guava-27.0-jre.jar" "$HIVE_HOME/lib/"
# Hadoop already provides an SLF4J binding; the second one only prints warnings.
rm -f "$HIVE_HOME/lib/log4j-slf4j-impl-2.10.0.jar"
# PostgreSQL JDBC driver for the metastore
[ -f "$HIVE_HOME/lib/$PG_JDBC" ] || wget -q -O "$HIVE_HOME/lib/$PG_JDBC" \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/$PG_JDBC"

echo "== 3. Metastore database in Postgres"
docker exec -i gitlytics-airflow-db psql -q -U "$AIRFLOW_DB_USER" -d airflow -v ON_ERROR_STOP=1 <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${HIVE_DB_USER}') THEN
    CREATE ROLE ${HIVE_DB_USER} LOGIN PASSWORD '${HIVE_DB_PASSWORD}';
  ELSE
    ALTER ROLE ${HIVE_DB_USER} WITH PASSWORD '${HIVE_DB_PASSWORD}';
  END IF;
END \$\$;
SQL
if ! docker exec gitlytics-airflow-db psql -U "$AIRFLOW_DB_USER" -d airflow -tAc \
        "SELECT 1 FROM pg_database WHERE datname = 'metastore'" | grep -q 1; then
    docker exec gitlytics-airflow-db psql -q -U "$AIRFLOW_DB_USER" -d airflow \
        -c "CREATE DATABASE metastore OWNER ${HIVE_DB_USER}"
fi

echo "== 4. hive-site.xml"
python3 - "$HIVE_HOME/conf/hive-site.xml" <<'PY'
import os, sys
from xml.sax.saxutils import escape

props = {
    "javax.jdo.option.ConnectionURL": "jdbc:postgresql://localhost:5433/metastore",
    "javax.jdo.option.ConnectionDriverName": "org.postgresql.Driver",
    "javax.jdo.option.ConnectionUserName": os.environ["HIVE_DB_USER"],
    "javax.jdo.option.ConnectionPassword": os.environ["HIVE_DB_PASSWORD"],
    "datanucleus.autoCreateSchema": "false",
    "hive.metastore.schema.verification": "true",
    "hive.metastore.warehouse.dir": "/user/hive/warehouse",
    "hive.exec.scratchdir": "/tmp/hive",
    "hive.execution.engine": "mr",
    "hive.exec.dynamic.partition.mode": "nonstrict",
    "hive.cli.print.header": "true",
    "hive.cli.print.current.db": "true",
    "hive.server2.enable.doAs": "false",
    "hive.server2.thrift.port": "10000",
    "hive.metastore.event.db.notification.api.auth": "false",
    # JSON SerDe for the bronze table; aux jars are also shipped to MapReduce tasks
    "hive.aux.jars.path": "file://" + os.path.expanduser(
        "~/bigdata/hive/hcatalog/share/hcatalog/hive-hcatalog-core-3.1.3.jar"),
}
body = "\n".join(
    f"  <property><name>{k}</name><value>{escape(v)}</value></property>" for k, v in props.items()
)
with open(sys.argv[1], "w") as f:
    f.write(f'<?xml version="1.0"?>\n<configuration>\n{body}\n</configuration>\n')
PY
chmod 600 "$HIVE_HOME/conf/hive-site.xml"

echo "== 5. HDFS directories"
"$HDFS" dfs -mkdir -p /user/hive/warehouse /tmp/hive
"$HDFS" dfs -chmod g+w /user/hive/warehouse
"$HDFS" dfs -chmod 1777 /tmp/hive

echo "== 6. Initialise metastore schema (first run only)"
export HADOOP_HOME HIVE_HOME
if "$HIVE_HOME/bin/schematool" -dbType postgres -info >/dev/null 2>&1; then
    echo "Metastore schema already initialised"
else
    "$HIVE_HOME/bin/schematool" -dbType postgres -initSchema
fi

echo "== 7. Smoke test"
"$HIVE_HOME/bin/hive" -S -e "CREATE DATABASE IF NOT EXISTS gitlytics; SHOW DATABASES;"
echo "Hive ${HIVE_VERSION} ready"
