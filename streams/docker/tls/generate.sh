#!/usr/bin/env bash
# Generates a throwaway certificate authority, a server certificate, and a
# client certificate for the TLS compose files and tests/*_e2e.rs.
#
#   streams/docker/tls/generate.sh
#
# Everything lands in streams/docker/tls/out/, which git ignores. These keys
# protect nothing: they exist so the e2e suite can prove every provider works
# over TLS with mutual authentication, and they are regenerated at will.
#
# Keys are unencrypted PKCS#8, the one form every broker here reads (Kafka's
# PEM keystore included), and world-readable so each broker image's own
# non-root user can open them from a read-only bind mount.
set -euo pipefail

out="$(cd "$(dirname "$0")" && pwd)/out"
mkdir -p "$out"
cd "$out"

days=30
subj="/O=tpp-streams e2e"

# The CA every party trusts.
openssl req -x509 -newkey rsa:2048 -nodes -days "$days" \
  -subj "$subj/CN=tpp-streams test CA" \
  -keyout ca.key -out ca.crt \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

issue() {
  local name="$1" cn="$2" usage="$3" san="$4"
  openssl req -newkey rsa:2048 -nodes -subj "$subj/CN=$cn" \
    -keyout "$name.key.tmp" -out "$name.csr" 2>/dev/null
  openssl pkcs8 -topk8 -nocrypt -in "$name.key.tmp" -out "$name.key"
  rm -f "$name.key.tmp"
  local ext
  ext="$(mktemp)"
  {
    echo "basicConstraints=critical,CA:FALSE"
    echo "keyUsage=critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage=$usage"
    [ -n "$san" ] && echo "subjectAltName=$san"
  } >"$ext"
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days "$days" -extfile "$ext" -out "$name.crt" 2>/dev/null
  rm -f "$name.csr" "$ext"
}

# The server certificate names every address a client or a sibling container
# might dial: the host loopback and each broker's compose service name.
issue server localhost serverAuth \
  "DNS:localhost,IP:127.0.0.1,DNS:kafka,DNS:nats,DNS:rabbitmq,DNS:redis,DNS:mqtt"
issue client tpp-streams-e2e clientAuth ""

# Some clients want the chain and key in one file.
cat server.crt ca.crt >server-chain.crt
chmod 644 ./*.crt ./*.key
rm -f ca.srl

echo "wrote $(ls "$out" | tr '\n' ' ')to $out"
