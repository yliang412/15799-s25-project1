#!/usr/bin/env bash

set -euo pipefail

WORKLOAD=$1
OUTPUT_DIR=$2


echo "Invoking optimize.sh."
echo -e "\tWorkload: ${WORKLOAD}"
echo -e "\tOutput Dir: ${OUTPUT_DIR}"

mkdir -p "${OUTPUT_DIR}"
mkdir -p input/

# Extract the workload.
tar xzf "${WORKLOAD}" --directory input/

# Feel free to add more steps here.

# Duckdb binary relative to the input directory.
DUCKDB_BIN="duckdb"
# Duckdb file relative to the input directory.
DUCKDB_FILE="15799-p1.db"

# Build and run the Calcite app.
cd calcite_app/

echo "Setting up ../${DUCKDB_FILE}, initialize schema and load data."
rm -f ../${DUCKDB_FILE}
../${DUCKDB_BIN} < ../input/data/schema.sql ../${DUCKDB_FILE}
../${DUCKDB_BIN} < ../input/data/load.sql ../${DUCKDB_FILE}

./gradlew build
./gradlew shadowJar
./gradlew --stop
java -Xmx4096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}"
cd -
