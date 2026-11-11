#!/usr/bin/env bash
# Runs the whole test suite locally: unit tests, offline KQL validation, and the emulator differential tests.
# Starts the Kusto emulator container if it isn't running. Usage:
#   scripts/test-local.sh            # all tests
#   scripts/test-local.sh --mutation # also run Stryker mutation testing (slow)
set -euo pipefail

CONTAINER=efcore-kusto-emulator
IMAGE=mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest
PORT=${KUSTO_EMULATOR_PORT:-18080}
export KUSTO_EMULATOR_URL="http://localhost:${PORT}"

cd "$(dirname "$0")/.."

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  echo "Starting the Kusto emulator..."
  docker run -d --name "$CONTAINER" --platform linux/amd64 -e ACCEPT_EULA=Y -p "${PORT}:8080" "$IMAGE" >/dev/null
fi

echo "Waiting for the emulator at ${KUSTO_EMULATOR_URL}..."
for _ in $(seq 1 120); do
  if curl -sf -X POST "${KUSTO_EMULATOR_URL}/v1/rest/mgmt" -H 'Content-Type: application/json' \
      -d '{"csl":".show version","db":"NetDefaultDB"}' >/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
if [ -z "${ready:-}" ]; then
  echo "The emulator did not become ready." >&2
  exit 1
fi

dotnet test EFCore.Kusto.sln

if [ "${1:-}" = "--mutation" ]; then
  dotnet tool restore
  dotnet stryker --config-file stryker-config.json
fi
