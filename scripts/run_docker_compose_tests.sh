#!/usr/bin/env bash
# scripts/run_docker_compose_tests.sh
# Automated helper to launch local Kafka cluster, run integration tests, and clean up.

set -euo pipefail

echo "================================================================================"
echo " Starting Biflux Local Kafka Cluster (Docker Compose)"
echo "================================================================================"

# 1. Spin up cluster
docker compose up -d

# 2. Wait for healthiness
echo "Waiting for Kafka broker to be healthy..."
RETRIES=30
until docker compose ps kafka | grep -q "healthy" || [ $RETRIES -eq 0 ]; do
    echo "Kafka initializing... ($RETRIES retries remaining)"
    sleep 1
    RETRIES=$((RETRIES - 1))
done

if [ $RETRIES -eq 0 ]; then
    echo "❌ Error: Kafka container failed to reach healthy state."
    docker compose logs kafka
    docker compose down
    exit 1
fi

echo "✅ Kafka broker is healthy on localhost:9092!"

# 3. Execute integration tests
echo "================================================================================"
echo " Running Docker Compose Integration Test Suite"
echo "================================================================================"
PYTEST_CMD="pytest"
if [ -f ".venv/bin/pytest" ]; then
    PYTEST_CMD=".venv/bin/pytest"
fi

set +e
"$PYTEST_CMD" integration_tests/test_docker_kafka_cluster.py -v -s
TEST_EXIT_CODE=$?
set -e

# 4. Tear down cluster
echo "================================================================================"
echo " Tearing down Local Kafka Cluster"
echo "================================================================================"
docker compose down

exit $TEST_EXIT_CODE
