#!/usr/bin/env bash
# Entrypoint for compose.kafka.secure.yaml: builds the PEM keystore, starts the
# image's own run script, then (re)applies the SCRAM user and the ACLs through
# the in-container plaintext listener. Every step is idempotent, so a restart
# repeats it harmlessly; the healthcheck waits for /tmp/auth-ready.
set -euo pipefail

cat /certs/server.key /certs/server-chain.crt > /tmp/keystore.pem
chmod 600 /tmp/keystore.pem
rm -f /tmp/auth-ready

/etc/kafka/docker/run &
broker=$!
# Pass docker stop/restart on to the broker so it shuts down cleanly.
trap 'kill -TERM "$broker" 2>/dev/null; wait "$broker"' TERM INT

bin=/opt/kafka/bin
internal=127.0.0.1:29092

setup() {
  until "$bin/kafka-topics.sh" --bootstrap-server "$internal" --list >/dev/null 2>&1; do
    sleep 1
  done

  # The SCRAM user, stored in the KRaft metadata log.
  "$bin/kafka-configs.sh" --bootstrap-server "$internal" --alter \
    --entity-type users --entity-name "$E2E_SCRAM_USER" \
    --add-config "SCRAM-SHA-512=[iterations=8192,password=$E2E_SCRAM_PASSWORD]"

  acl() { "$bin/kafka-acls.sh" --bootstrap-server "$internal" --add "$@" >/dev/null; }

  # The SCRAM user: exactly what the driver needs, under its prefix only.
  # Topics: create (declare/update), describe (metadata), read, write, delete.
  local scram="User:$E2E_SCRAM_USER"
  acl --allow-principal "$scram" --resource-pattern-type prefixed \
    --topic "$E2E_SCRAM_PREFIX" \
    --operation Create --operation Describe --operation Read \
    --operation Write --operation Delete
  # Groups: named consumers under the prefix, and the throwaway
  # streams-reader-<id> group every tail and metadata read assigns under.
  for group in "$E2E_SCRAM_PREFIX" streams-reader-; do
    acl --allow-principal "$scram" --resource-pattern-type prefixed \
      --group "$group" --operation Read --operation Describe
  done

  # The mutual-TLS client (its certificate CN, see ssl.principal.mapping.rules):
  # the existing suite uses arbitrary prefixes and group names and lists
  # groups, so it gets every topic and group and cluster describe.
  local mtls="User:tpp-streams-e2e"
  acl --allow-principal "$mtls" --topic '*' --operation All
  acl --allow-principal "$mtls" --group '*' --operation All
  acl --allow-principal "$mtls" --cluster --operation Describe

  touch /tmp/auth-ready
  echo "===> SCRAM user and ACLs applied"
}

setup &
wait "$broker"
