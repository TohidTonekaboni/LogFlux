#!/bin/sh
set -eu

echo "Waiting for Kibana to be ready..."
until curl -sS -u "elastic:${ELASTIC_PASSWORD}" \
  "http://kibana:5601/api/status" | grep -q '"level":"available"'; do
  sleep 5
done

echo "Importing saved objects (index pattern, searches, dashboard)..."
curl -sS --fail-with-body -u "elastic:${ELASTIC_PASSWORD}" \
  -X POST "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F "file=@/saved_objects.ndjson"
echo

echo "kibana-setup complete."
