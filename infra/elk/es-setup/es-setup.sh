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


echo "Creating ILM policy..."
curl -sS --fail-with-body -u "elastic:${ELASTIC_PASSWORD}" -X PUT \
  "http://elasticsearch:9200/_ilm/policy/logflux-logs-policy" \
  -H 'Content-Type: application/json' \
  -d '{
    "policy": {
      "phases": {
        "hot":    { "min_age": "0ms", "actions": { "set_priority": { "priority": 100 } } },
        "warm":   { "min_age": "7d",  "actions": { "set_priority": { "priority": 50 } } },
        "delete": { "min_age": "30d", "actions": { "delete": {} } }
      }
    }
  }'
echo

echo "Creating logs-* index template..."
curl -sS --fail-with-body -u "elastic:${ELASTIC_PASSWORD}" -X PUT \
  "http://elasticsearch:9200/_index_template/logflux-logs-template" \
  -H 'Content-Type: application/json' \
  -d '{
    "index_patterns": ["logs-*"],
    "template": {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "index.lifecycle.name": "logflux-logs-policy"
      },
      "mappings": {
        "properties": {
          "timestamp":    { "type": "date" },
          "level":        { "type": "keyword" },
          "service":      { "type": "keyword" },
          "environment":  { "type": "keyword" },
          "trace_id":     { "type": "keyword" },
          "message":      { "type": "text" },
          "fields":       { "type": "object" }
        }
      }
    }
  }'
echo

echo "es-setup complete."
