#!/usr/bin/env bash
# Issues the OPC UA application certificates for compose.opcua.secure.yaml and
# tests/opcua_e2e.rs:
#
#   streams/docker/compose.opcua.secure.sh          # once, before `up`
#   streams/docker/compose.opcua.secure.sh --force  # to issue new ones
#
# Everything lands in streams/docker/tls/out/opcua/, which git ignores. These
# keys protect nothing; they exist so the e2e suite can prove the provider
# works over a signed and encrypted channel with real certificate trust.
#
# OPC UA authenticates applications, not hosts: each side presents an
# application instance certificate, self-signed here as most OPC UA
# deployments are, and each trusts the other's by putting it in its trusted
# store. So three are issued:
#
#   server    opc-plc's own (--af/--pk), which the client pins in its store
#   client    the test client's, which opc-plc is given as trusted (--tf)
#   stranger  a client nobody trusts, to prove an untrusted one is refused
#
# What OPC UA requires of an application certificate (Part 6, 6.2.2): the
# application URI as a URI subjectAltName, the host names it is reached by,
# RSA 2048 with SHA-256 for Basic256Sha256 and Aes256Sha256RsaPss, and the
# key usages below.
set -euo pipefail

out="$(cd "$(dirname "$0")" && pwd)/tls/out/opcua"

# Whether $out holds everything below, with a day left on every certificate:
# a run cut short, or certificates past their 30 days, are issued again.
complete() {
  local name thumb
  for name in server client stranger; do
    [[ -f "$out/$name.der" && -f "$out/$name.pem" ]] || return 1
    openssl x509 -inform DER -in "$out/$name.der" -noout -checkend 86400 >/dev/null 2>&1 || return 1
  done
  thumb="$(openssl x509 -inform DER -in "$out/server.der" -noout -fingerprint -sha1 | sed 's/.*=//; s/://g')"
  [[ -f "$out/own/certs/OpcPlc [$thumb].der" && -f "$out/own/private/OpcPlc [$thumb].pfx" ]]
}

if [[ "${1:-}" != "--force" ]] && complete; then
  echo "certificates already in $out (pass --force to issue new ones)"
  exit 0
fi
rm -rf "$out"
mkdir -p "$out"
cd "$out"

days=30

issue() {
  local name="$1" subject="$2" uri="$3"
  local ext
  ext="$(mktemp)"
  cat > "$ext" <<CONF
[req]
distinguished_name = dn
prompt = no
x509_extensions = v3
[dn]
$subject
[v3]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,nonRepudiation,keyEncipherment,dataEncipherment,keyCertSign
extendedKeyUsage = serverAuth,clientAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
subjectAltName = URI:$uri,DNS:localhost,DNS:tpp-streams-opcua-secure,IP:127.0.0.1
CONF
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days "$days" \
    -config "$ext" -keyout "$name.pem" -out "$name.crt" 2>/dev/null
  openssl x509 -in "$name.crt" -outform DER -out "$name.der"
  rm -f "$ext" "$name.crt"
}

# opc-plc finds its application certificate by subject, CN=OpcPlc, and makes
# itself a new one when the store has none under that name, so the issued one
# carries exactly that subject. Its application URI is urn:OpcPlc:<hostname>;
# the compose file fixes the hostname, so the certificate can name it.
issue server "CN = OpcPlc" "urn:OpcPlc:tpp-streams-opcua-secure"
issue client "$(printf 'O = tpp-streams e2e\nCN = tpp-streams e2e client')" "urn:tpp-streams:e2e-client"
issue stranger "$(printf 'O = tpp-streams e2e\nCN = tpp-streams stranger')" "urn:tpp-streams:e2e-client"

# opc-plc's own store, laid out as the .NET stack's directory store keeps it:
# the certificate in certs/ and the key, as PKCS#12, in private/, both named
# by subject and thumbprint. Mounted as the store, it is the certificate the
# server presents; opc-plc's --af/--pk import is not used because opc-plc then
# looks for its certificate elsewhere and issues itself a new one.
thumb="$(openssl x509 -inform DER -in server.der -noout -fingerprint -sha1 | sed 's/.*=//; s/://g')"
mkdir -p own/certs own/private
cp server.der "own/certs/OpcPlc [$thumb].der"
openssl x509 -inform DER -in server.der -out server.crt
openssl pkcs12 -export -inkey server.pem -in server.crt -name OpcPlc \
  -out "own/private/OpcPlc [$thumb].pfx" -passout pass:
rm -f server.crt

# Readable (and the store writable) by the image's non-root user.
chmod 644 ./*.der ./*.pem own/certs/* own/private/*
chmod 777 own own/certs own/private
echo "issued server, client and stranger certificates in $out"
