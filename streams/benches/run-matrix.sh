#!/usr/bin/env bash
# Runs the official throughput matrix, one backend at a time so runs never
# compete for the machine. Results append to target/bench-results.jsonl.
#
#   streams/benches/run-matrix.sh [backend ...]      # default: every backend
#
# Each paced run sends 5 seconds' worth of messages at the target rate (at
# least 100,000); the unpaced run sends 500,000 to find the ceiling. Profiles:
#   plain        plaintext broker (docker/compose.<b>.yaml)
#   tls-prod     secure broker, mTLS + auth, production durability
#   tls-strict   secure broker, mTLS + auth, sync every message to disk
set -uo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
docker_dir="$root/streams/docker"
label="${MATRIX_LABEL:-matrix-$(date +%Y%m%d-%H%M)}"
rates=(${MATRIX_RATES:-20000 50000 100000 0})

# backend -> "<cargo feature> <compose name> <scenarios>". A function, not an
# associative array, because macOS ships bash 3.2.
spec() {
  case "$1" in
    zeromq) echo "zeromq - fanout batch" ;;
    nats) echo "nats nats fanout batch group" ;;
    jetstream) echo "nats nats fanout batch durable" ;;
    redis) echo "redis redis fanout batch" ;;
    redis-durable) echo "redis redis fanout batch durable" ;;
    kafka) echo "kafka kafka fanout batch durable group" ;;
    rabbitmq) echo "rabbitmq rabbitmq fanout batch durable" ;;
    mqtt) echo "mqtt mqtt fanout batch durable group" ;;
    *) echo "unknown backend $1" >&2; exit 2 ;;
  esac
}
order=(zeromq nats jetstream redis redis-durable kafka rabbitmq mqtt)
[ $# -gt 0 ] && order=("$@")

up() {   # compose-name profile
  local c="$1" p="$2"
  case "$p" in
    plain) docker compose -p "tpp-streams-live-$c" -f "$docker_dir/compose.$c.yaml" up -d --wait >/dev/null ;;
    tls-prod) STREAMS_DURABILITY=prod docker compose -p "tpp-streams-e2e-$c" -f "$docker_dir/compose.$c.secure.yaml" up -d --wait --force-recreate >/dev/null ;;
    tls-strict) STREAMS_DURABILITY=strict docker compose -p "tpp-streams-e2e-$c" -f "$docker_dir/compose.$c.secure.yaml" up -d --wait --force-recreate >/dev/null ;;
  esac
}

down() { # compose-name
  docker compose -p "tpp-streams-live-$1" -f "$docker_dir/compose.$1.yaml" down -v >/dev/null 2>&1
  docker compose -p "tpp-streams-e2e-$1" -f "$docker_dir/compose.$1.secure.yaml" down -v >/dev/null 2>&1
}

# Publishers that keep enough publishes in flight. Every confirmed publish waits
# about a round trip, so 32 publishers cap a backend near 32k/s whatever it
# could do; batches need far fewer because each call carries --batch messages.
publishers() { # backend scenario
  if [ "$2" = batch ]; then echo 32; return; fi
  case "$1" in mqtt|kafka) echo 512 ;; *) echo 256 ;; esac
}

run() {  # feature backend profile scenario rate
  local feature="$1" backend="$2" profile="$3" scenario="$4" rate="$5" messages
  if [ "$rate" = 0 ]; then messages=500000; else messages=$(( rate * 5 > 100000 ? rate * 5 : 100000 )); fi
  # Syncing every message to disk runs at hundreds to a few thousand a
  # second, so strict gets one unpaced measurement of a smaller run.
  [ "$profile" = tls-strict ] && messages=20000
  local cli_profile="$profile"; [ "$profile" != plain ] && cli_profile=tls
  printf '%-14s %-10s %-8s %7s/s ... ' "$backend" "$profile" "$scenario" "$rate"
  cargo bench -q -p tpp-streams --features "kafka,nats,rabbitmq,redis,mqtt,zeromq,protobuf,live-tests,tls" --bench throughput -- \
    --backend "$backend" --profile "$cli_profile" --scenario "$scenario" \
    --messages "$messages" --rate "$rate" --publishers "$(publishers "$backend" "$scenario")" --batch 500 --consumers "$([ "$scenario" = group ] || [ "$scenario" = durable ] && echo 2 || echo 1)" \
    --timeout 180 --label "$label/$profile" >/dev/null 2>"$root/target/bench-last.log" \
    || { echo "FAIL (no result; see target/bench-last.log)"; return; }
  tail -1 "$root/target/bench-results.jsonl" | python3 -c '
import sys, json
d = json.loads(sys.stdin.read())
l = d["latency_ms"]
status = "OK  " if d["ok"] else "FAIL"
line = "%s delivered %10.0f/s  lost %d  dup %d  p50 %s p99 %s ms" % (
    status, d["delivered_rate"], d["lost"], d["duplicates"], l["p50"], l["p99"])
if d["first_error"]:
    line += "  err: " + d["first_error"]
print(line)'
}

cd "$root"
for backend in "${order[@]}"; do
  read -r feature compose scenarios <<<"$(spec "$backend")"
  profiles=(plain)
  [ "$compose" != - ] && profiles+=(tls-prod tls-strict)
  for profile in "${profiles[@]}"; do
    [ "$compose" != - ] && { down "$compose"; up "$compose" "$profile"; }
    for scenario in $scenarios; do
      profile_rates=("${rates[@]}")
      [ "$profile" = tls-strict ] && profile_rates=(0)
      for rate in "${profile_rates[@]}"; do
        run "$feature" "$backend" "$profile" "$scenario" "$rate"
      done
    done
  done
  [ "$compose" != - ] && down "$compose"
done
echo "results: $root/target/bench-results.jsonl (label $label)"
