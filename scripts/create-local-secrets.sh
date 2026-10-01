#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
umask 077
mkdir -p .secrets/mosquitto
chmod 700 .secrets .secrets/mosquitto

for path in .secrets/dynsec_admin_password .secrets/receiver_password .secrets/db_password; do
	if [ ! -s "$path" ]; then
		openssl rand -base64 32 > "$path"
	fi
	chmod 600 "$path"
done
# Compose file secrets are bind mounts; non-root containers need read access.
# The containing host directories remain mode 700.
chmod 644 .secrets/dynsec_admin_password .secrets/receiver_password .secrets/db_password

cert_dir=.secrets/mosquitto
if [ -s "$cert_dir/ca.crt" ] && [ -s "$cert_dir/ca.key" ] && [ -s "$cert_dir/server.crt" ] && [ -s "$cert_dir/server.key" ]; then
	chmod 600 "$cert_dir/ca.key"
	chmod 644 "$cert_dir/ca.crt" "$cert_dir/server.crt" "$cert_dir/server.key"
	exit 0
fi
if [ -e "$cert_dir/ca.crt" ] || [ -e "$cert_dir/ca.key" ] || [ -e "$cert_dir/server.crt" ] || [ -e "$cert_dir/server.key" ]; then
	printf '%s\n' "证书目录不完整；为避免覆盖现有 Broker 信任根，请先备份并检查 $cert_dir。" >&2
	exit 1
fi

dns_name=${MQTT_CERT_DNS_NAME:-localhost}
ip_address=${MQTT_CERT_IP:-127.0.0.1}
cat > "$cert_dir/ca.cnf" <<'EOF'
[req]
prompt = no
distinguished_name = dn
x509_extensions = v3_ca
[dn]
CN = Project01 local MQTT CA
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF

cat > "$cert_dir/server.cnf" <<EOF
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

openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 3650 \
	-config "$cert_dir/ca.cnf" \
	-keyout "$cert_dir/ca.key" -out "$cert_dir/ca.crt"
openssl req -newkey rsa:3072 -nodes -sha256 \
	-config "$cert_dir/server.cnf" \
	-keyout "$cert_dir/server.key" -out "$cert_dir/server.csr"
openssl x509 -req -sha256 -days 825 \
	-in "$cert_dir/server.csr" \
	-CA "$cert_dir/ca.crt" -CAkey "$cert_dir/ca.key" -CAcreateserial \
	-extfile "$cert_dir/server.cnf" -extensions server_cert \
	-out "$cert_dir/server.crt"
chmod 600 "$cert_dir"/*
chmod 644 "$cert_dir/ca.crt" "$cert_dir/server.crt" "$cert_dir/server.key"
printf '%s\n' "本机 Broker 密钥和 CA 已生成在 .secrets/（Git 已忽略）。"
