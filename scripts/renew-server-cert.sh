#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
umask 077
cert_dir=.secrets/mosquitto
dns_name=${MQTT_CERT_DNS_NAME:?Set MQTT_CERT_DNS_NAME to the device-facing hostname}
ip_address=${MQTT_CERT_IP:-10.0.0.113}
case "$dns_name" in
  ''|*[!a-zA-Z0-9.-]*) printf '%s\n' 'Invalid DNS name' >&2; exit 1 ;;
esac
case "$ip_address" in
  ''|*[!0-9.]*) printf '%s\n' 'Expected an IPv4 address' >&2; exit 1 ;;
esac
test -s "$cert_dir/ca.crt"
test -s "$cert_dir/ca.key"

staging=$(mktemp -d "$cert_dir/.renew.XXXXXX")
trap 'rm -rf "$staging"' EXIT HUP INT TERM
cat > "$staging/server.cnf" <<EOF
[req]
prompt = no
distinguished_name = dn
req_extensions = server_ext
[dn]
CN = $dns_name
[server_ext]
subjectAltName = @alt_names
[server_cert]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = $dns_name
DNS.2 = localhost
DNS.3 = broker
IP.1 = $ip_address
IP.2 = 127.0.0.1
EOF

openssl req -new -newkey rsa:3072 -nodes -sha256 \
  -config "$staging/server.cnf" \
  -keyout "$staging/server.key" -out "$staging/server.csr" 2>/dev/null
serial=$(openssl rand -hex 16)
openssl x509 -req -sha256 -days 365 -set_serial "0x$serial" \
  -in "$staging/server.csr" -CA "$cert_dir/ca.crt" -CAkey "$cert_dir/ca.key" \
  -extfile "$staging/server.cnf" -extensions server_cert \
  -out "$staging/server.crt"
for name in "$dns_name" localhost broker; do
  openssl verify -CAfile "$cert_dir/ca.crt" -verify_hostname "$name" "$staging/server.crt"
done
openssl verify -CAfile "$cert_dir/ca.crt" -verify_ip "$ip_address" "$staging/server.crt"

backup=$(mktemp -d "$cert_dir/backup-$(date +%Y%m%d-%H%M%S).XXXXXX")
for name in server.crt server.key server.csr server.cnf; do
  if [ -f "$cert_dir/$name" ]; then cp -p "$cert_dir/$name" "$backup/"; fi
done
chmod 644 "$staging/server.crt" "$staging/server.key" "$cert_dir/ca.crt"
for name in server.crt server.key server.csr server.cnf; do
  mv "$staging/$name" "$cert_dir/$name"
done
printf 'Issued server certificate for %s and %s; previous files: %s\n' "$dns_name" "$ip_address" "$backup"
printf '%s\n' 'Recreate the broker container to load the new certificate. The CA and Broker volume are unchanged.'
