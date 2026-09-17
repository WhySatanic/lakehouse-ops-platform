#!/usr/bin/env bash
set -euo pipefail

security_dir=/security
keystore="$security_dir/trino.p12"
certificate="$security_dir/trino.crt"
: "${TRINO_TLS_KEYSTORE_PASSWORD:?TRINO_TLS_KEYSTORE_PASSWORD is required}"

mkdir -p "$security_dir"
umask 077

if [[ ! -f "$keystore" ]]; then
  keytool -genkeypair \
    -alias trino \
    -keyalg RSA \
    -keysize 3072 \
    -validity 30 \
    -dname "CN=trino-secure-coordinator,OU=Lakehouse Ops,O=Local Development,L=Local,ST=Local,C=XX" \
    -ext "SAN=dns:trino-secure-coordinator,dns:localhost" \
    -storetype PKCS12 \
    -keystore "$keystore" \
    -storepass "$TRINO_TLS_KEYSTORE_PASSWORD" \
    -keypass "$TRINO_TLS_KEYSTORE_PASSWORD" \
    -noprompt
fi

keytool -list -alias trino -keystore "$keystore" \
  -storepass "$TRINO_TLS_KEYSTORE_PASSWORD" >/dev/null
keytool -exportcert -rfc -alias trino -keystore "$keystore" \
  -storepass "$TRINO_TLS_KEYSTORE_PASSWORD" -file "$certificate"
chmod 0444 "$keystore" "$certificate"
