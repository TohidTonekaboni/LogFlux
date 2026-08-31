#!/bin/sh
# One-shot: runs once per `docker compose up`, idempotent (PUT on the user is an
# upsert, and resetting kibana_system's password is harmless even if unchanged).
set -eu

echo "Setting kibana_system password..."
curl -sS --fail-with-body -u "elastic:${ELASTIC_PASSWORD}" -X POST \
  "http://elasticsearch:9200/_security/user/kibana_system/_password" \
  -H 'Content-Type: application/json' \
  -d "{\"password\":\"${KIBANA_SYSTEM_PASSWORD}\"}"
echo

echo "Creating/updating ${ES_USER} user..."
curl -sS --fail-with-body -u "elastic:${ELASTIC_PASSWORD}" -X PUT \
  "http://elasticsearch:9200/_security/user/${ES_USER}" \
  -H 'Content-Type: application/json' \
  -d "{\"password\":\"${ES_PASSWORD}\",\"roles\":[\"superuser\"],\"full_name\":\"AI Fashion Admin\"}"
echo

echo "es-setup complete."
