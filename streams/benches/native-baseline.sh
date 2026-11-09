#!/usr/bin/env bash
# The broker's own ceiling, measured with each project's official tool over the
# same network path the Rust driver uses, so the driver can be judged as a
# share of it rather than against a figure measured somewhere else.
#
#   streams/benches/native-baseline.sh            # every backend below
#   streams/benches/native-baseline.sh kafka
#
# Docker Desktop on a Mac forwards published ports from the Mac into its VM,
# and that forward, not the broker, is the narrowest part of the path. A tool
# run inside Docker's own network skips it and reports several times what any
# client on the Mac can reach, so every tool here connects through
# host.docker.internal, which takes the same forward back in.
#
# Messages are 256 bytes, the size the throughput bench publishes.
set -uo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
docker_dir="$root/streams/docker"
messages="${BASELINE_MESSAGES:-1000000}"
order=(nats kafka rabbitmq mqtt)
[ $# -gt 0 ] && order=("$@")

# Any failure, startup or measurement, is recorded here and becomes the exit
# status once everything is torn down, so cleanup that succeeds cannot hide
# a run that did not.
failed=0
fail() { echo "FAILED: $*" >&2; failed=1; }
# Exit status of a tool whose output went through a filter: the filter's own
# status says nothing about whether the tool ran.
check() { [ "$1" -eq 0 ] || fail "$2 (exit $1)"; }

# Compose stacks this run started, as "project|file". Only these are torn down
# on exit or interrupt: a stack that was already up (the matrix runner uses the
# same project names) belongs to whoever started it.
started=()
up() { # project file
  if docker compose -p "$1" -f "$2" ps -q 2>/dev/null | grep -q .; then
    return 0
  fi
  docker compose -p "$1" -f "$2" up -d --wait >/dev/null 2>&1 || { fail "starting $1"; return 1; }
  started+=("$1|$2")
}
down() { # project file
  local keep=() s
  for s in "${started[@]+"${started[@]}"}"; do
    if [ "$s" = "$1|$2" ]; then
      docker compose -p "$1" -f "$2" down -v >/dev/null 2>&1
    else
      keep+=("$s")
    fi
  done
  started=("${keep[@]+"${keep[@]}"}")
}
cleanup() {
  local s
  for s in "${started[@]+"${started[@]}"}"; do
    docker compose -p "${s%%|*}" -f "${s#*|}" down -v >/dev/null 2>&1
  done
  started=()
}
trap cleanup EXIT
trap 'fail "interrupted"; exit 130' INT TERM

nats_box() { docker run --rm natsio/nats-box:latest "$@"; }

nats() {
  local url=nats://host.docker.internal:14222
  up tpp-streams-live-nats "$docker_dir/compose.nats.yaml" || return
  echo "== NATS core: one publisher, one subscriber"
  docker run --rm natsio/nats-box:latest sh -c "nats bench sub bench.baseline --server $url --msgs $messages --size 256 --clients 1 --no-progress & s=\$!; sleep 1; nats bench pub bench.baseline --server $url --msgs $messages --size 256 --clients 1 --no-progress; p=\$?; wait \$s; q=\$?; [ \$p -eq 0 ] && [ \$q -eq 0 ]" 2>&1 | grep -i "stats"
  check "${PIPESTATUS[0]}" "nats bench core"
  # JetStream runs are smaller: a million file-backed messages pushed through
  # the forward has taken Docker Desktop down on an 8 GB VM.
  for ids in "" --dedup; do
    nats_box nats stream rm benchstream -f --server "$url" >/dev/null 2>&1
    echo "== NATS JetStream: async publish, 500 in flight ${ids:+(with message ids, as the driver sends)}"
    nats_box nats bench js pub async bench.baseline $ids --server "$url" --msgs 200000 --size 256 \
      --batch 500 --clients 1 --create --purge --no-progress 2>&1 | grep -iE "publisher stats|error"
    check "${PIPESTATUS[0]}" "nats bench js pub async $ids"
  done
  nats_box nats stream rm benchstream -f --server "$url" >/dev/null 2>&1
  down tpp-streams-live-nats "$docker_dir/compose.nats.yaml"
}

kafka() {
  local bootstrap=host.docker.internal:19095 image
  up tpp-streams-live-kafka "$docker_dir/compose.kafka.yaml" || return
  image="$(docker inspect tpp-streams-kafka --format '{{.Config.Image}}')"
  # Each tool's output goes to a file and its status is checked before the
  # last line is shown: a pipe into tail would report tail's status instead.
  docker run --rm --entrypoint sh "$image" -c "
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server $bootstrap --create --topic baseline256 --partitions 10 --replication-factor 1 >/dev/null || exit 1
    echo '== Kafka producer: acks=all, idempotent, as the driver produces'
    /opt/kafka/bin/kafka-producer-perf-test.sh --topic baseline256 --num-records $messages --record-size 256 --throughput -1 \
      --producer-props bootstrap.servers=$bootstrap acks=all enable.idempotence=true linger.ms=1 batch.size=65536 > /tmp/p.log 2>&1 || { cat /tmp/p.log; exit 1; }
    tail -1 /tmp/p.log
    echo '== Kafka consumer'
    /opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server $bootstrap --topic baseline256 --messages $messages --timeout 60000 > /tmp/c.log 2>&1 || { cat /tmp/c.log; exit 1; }
    tail -1 /tmp/c.log
  " 2>&1 | grep -v WARN
  check "${PIPESTATUS[0]}" "kafka perf tools"
  down tpp-streams-live-kafka "$docker_dir/compose.kafka.yaml"
}

# RabbitMQ's own load tool (pivotalrabbitmq/perf-test), through the forward.
# Only port 15682 (docker/compose.rabbitmq.yaml) is used, never 5672, which may
# be another RabbitMQ on the same machine. One producer and one consumer on one
# queue, 256-byte bodies; the JVM is capped so it fits beside other brokers in
# Docker Desktop's VM.
rabbitmq() {
  local url=amqp://guest:guest@host.docker.internal:15682 n=$(( messages < 500000 ? messages : 500000 ))
  up tpp-streams-live-rabbitmq "$docker_dir/compose.rabbitmq.yaml" || return
  perf() {
    local label=$1; shift
    echo "== RabbitMQ: $label"
    docker run --rm -e JAVA_OPTS=-Xmx512m pivotalrabbitmq/perf-test:latest --uri "$url" \
      -x 1 -y 1 -s 256 -C "$n" -D "$n" "$@" 2>&1 | grep -E "rate avg|Exception|error" | sed 's/^id: [^,]*, //'
    check "${PIPESTATUS[0]}" "perf-test: $label"
  }
  # The broker's raw ceiling: transient, unconfirmed, auto-acknowledged.
  perf "raw ceiling: transient, no confirms, auto-ack" -a
  # As the driver's fanout: persistent, confirmed, into an exclusive
  # server-named queue consumed with auto-ack. 32 in flight is the harness's 32
  # publishers each awaiting one confirm; 500 is one batch's worth.
  perf "fanout: persistent, confirm 32, transient queue, auto-ack" -f persistent -c 32 -a
  perf "batch: persistent, confirm 500, transient queue, auto-ack" -f persistent -c 500 -a
  # As the driver's durable consumer: a durable classic queue, a confirm only
  # after the queue's fsync, and per-message acks under a 1024 prefetch.
  perf "durable: persistent, confirm 32, durable queue, qos 1024, per-message acks" \
    -f persistent -c 32 -q 1024 -u baseline.durable32 -ad false --queue-args x-queue-type=classic
  perf "durable: persistent, confirm 500, durable queue, qos 1024, per-message acks" \
    -f persistent -c 500 -q 1024 -u baseline.durable500 -ad false --queue-args x-queue-type=classic
  down tpp-streams-live-rabbitmq "$docker_dir/compose.rabbitmq.yaml"
}

# MQTT's load tool (emqx/emqtt-bench) against Mosquitto, through the forward.
# MQTT 5, 256-byte payloads, on docker/compose.mqtt.yaml's plaintext broker
# (port 11883). Subscribers and publishers run in one container; the
# subscribers connect first, so nothing is published before they are attached.
#
# emqtt-bench prints a rate per counter every second. The steady-state figure
# is the median of those samples once the first (connecting, ramping) and the
# last (a partial second) are dropped. A run over in under three seconds, as
# QoS 0 publishing can be, has no steady state, and its best second is shown
# as the peak instead. `pub_succ` counts publishes the broker acknowledged,
# `pub` publishes written (QoS 0 has nothing else), `recv` deliveries.
#
# emqtt-bench 0.6.3 keeps one QoS 1 publish outstanding per connection when
# publishing unpaced (-I 0): its -F (in-flight) setting made no difference
# between 1 and 1000 here. Its QoS 1 rate is therefore set by how many
# connections publish at once, each waiting for its PUBACK, which is how the
# harness's single-publish scenarios work too (each publisher awaits one
# PUBACK). MQTT_BASELINE_PUBLISHERS sets that count (default 1000).
mqtt() {
  local n=$(( messages < 500000 ? messages : 500000 )) pubs="${MQTT_BASELINE_PUBLISHERS:-1000}"
  up tpp-streams-live-mqtt "$docker_dir/compose.mqtt.yaml" || return
  # eb <label> <counter> <subscribers> <sub topic> <publishers> <pub topic> <qos> <messages>
  eb() {
    echo "== MQTT: $1"
    docker run --rm --entrypoint sh emqx/emqtt-bench:latest -c '
      b=/emqtt_bench/bin/emqtt_bench; at="-h host.docker.internal -p 11883 -V 5 -A true"
      $b sub $at -c "$3" -t "$4" -q "$7" > /tmp/sub.log 2>&1 &
      sleep 2
      $b pub $at -c "$5" -R 1000 -w true -t "$6" -q "$7" -s 256 -I 0 -L "$8" > /tmp/pub.log 2>&1
      published=$?
      # Let the subscribers drain, then stop once deliveries stop rising.
      last=-1
      while :; do
        sleep 2
        now=$(grep " recv total=" /tmp/sub.log | tail -1 | sed "s/.*total=\([0-9]*\).*/\1/")
        [ "${now:-0}" = "$last" ] && break; last=${now:-0}
      done
      kill $! 2>/dev/null
      steady() {
        r=$(grep "s $1 total=" "$2" | sed "s/.*rate=\([0-9.]*\).*/\1/")
        if [ "$(echo "$r" | grep -c .)" -ge 3 ]; then
          echo "$r" | sed "1d;\$d" | sort -n | awk "{ r[NR] = \$1 } END { printf \"%.0f msg/s steady\", r[int((NR + 1) / 2)] }"
        elif [ -n "$r" ]; then
          echo "$r" | sort -n | tail -1 | awk "{ printf \"%.0f msg/s peak\", \$1 }"
        else
          printf "n/a"
        fi
      }
      echo "   publish $(steady "$2" /tmp/pub.log), delivery $(steady recv /tmp/sub.log)," \
        "delivered ${last} of $8"
      exit $published
    ' eb "$@" 2>&1 | grep -E "^==|^   |rror"
    check "${PIPESTATUS[0]}" "emqtt-bench: $1"
  }
  # Topics: publishers pick bench/fan/<1..10> at random per message; the
  # subscribers take bench/fan/0..10 by their sequence number, one each, so
  # every topic has exactly one subscriber whichever end of 0..10 rand uses.
  eb "(a) QoS 1, one publisher, one subscriber, one topic" \
    pub_succ 1 bench/one 1 bench/one 1 $(( n < 30000 ? n : 30000 ))
  eb "(b) QoS 1, $pubs publishers over 10 topics, one subscriber each (the harness's fanout)" \
    pub_succ 11 'bench/fan/%i' "$pubs" 'bench/fan/%rand_10' 1 "$n"
  eb "(c) QoS 0, one publisher, one subscriber, one topic (raw ceiling)" \
    pub 1 bench/one 1 bench/one 0 500000
  eb "(c) QoS 0, 10 publishers over 10 topics, one subscriber each (raw ceiling)" \
    pub 11 'bench/fan/%i' 10 'bench/fan/%rand_10' 0 500000
  down tpp-streams-live-mqtt "$docker_dir/compose.mqtt.yaml"
}

for b in "${order[@]}"; do "$b"; done
cleanup
exit "$failed"
