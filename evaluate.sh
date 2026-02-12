#!/bin/bash
set -eo pipefail

CREATE_CLASS="org.example.CreateMeasurements"
CALC_CLASS="org.example.CalculateAverage"

MEASUREMENTS_FILE="src/test/resources/measurements.txt"
MEASUREMENTS_SIZE="${1:-10000000}"
RUNS="${2:-5}"

echo "Settings:"
echo "  MEASUREMENTS_FILE = $MEASUREMENTS_FILE"
echo "  MEASUREMENTS_SIZE = $MEASUREMENTS_SIZE"
echo "  RUNS              = $RUNS"
echo ""

if ! command -v java >/dev/null 2>&1; then
  echo "Error: java is not installed or not in PATH"
  exit 1
fi

echo "Compiling classes..."
javac -encoding UTF-8 -cp src -d out \
  src/java/org/example/CreateMeasurements.java \
  src/java/org/example/CalculateAverage.java \
  src/java/org/example/StationStats.java \
  src/java/org/example/StationKey.java



CP="out"
echo "Using classpath: $CP"
echo ""

echo "Creating measurements file..."
echo "  java -cp \"$CP\" $CREATE_CLASS $MEASUREMENTS_SIZE"
java -cp "$CP" "$CREATE_CLASS" "$MEASUREMENTS_SIZE"
echo "Done creating $MEASUREMENTS_FILE"
echo ""

if [ ! -f "$MEASUREMENTS_FILE" ]; then
  echo "Error: $MEASUREMENTS_FILE not found after generation."
  exit 1
fi

echo "Running $CALC_CLASS $RUNS time(s)..."
echo ""

BEST_MS=999999999
TOTAL_MS=0

for i in $(seq 1 "$RUNS"); do
  echo "Run #$i"
  start_ns=$(date +%s%N)

  java -cp "$CP" "$CALC_CLASS"


  end_ns=$(date +%s%N)
  elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
  echo "  Time: ${elapsed_ms} ms"
  echo ""

  TOTAL_MS=$((TOTAL_MS + elapsed_ms))
  if [ "$elapsed_ms" -lt "$BEST_MS" ]; then
    BEST_MS=$elapsed_ms
  fi
done

AVG_MS=$((TOTAL_MS / RUNS))

echo "Summary:"
echo "  Runs:      $RUNS"
echo "  Best time: ${BEST_MS} ms"
echo "  Avg time:  ${AVG_MS} ms"
echo ""
