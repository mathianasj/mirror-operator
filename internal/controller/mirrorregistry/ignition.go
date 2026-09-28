package mirrorregistry

import (
	"encoding/base64"
	"fmt"
)

func buildIgnitionFiles(cfg *Config) []interface{} {
	files := []interface{}{
		ignitionFile(cfg.DataPath+"/quay-config/config.yaml", 0644, buildQuayConfig(cfg)),
		ignitionFile("/usr/local/bin/mirror-registry-init.sh", 0755, buildInitScript(cfg)),
		ignitionFile("/usr/local/bin/mirror-registry-admin-init.sh", 0755, buildAdminInitScript(cfg)),
	}

	if cfg.CACertPEM != "" {
		files = append(files,
			ignitionFile(cfg.DataPath+"/ca/ca.crt", 0644, cfg.CACertPEM),
			ignitionFile(cfg.DataPath+"/ca/ca.key", 0600, cfg.CAKeyPEM),
		)
	}

	if cfg.PullSecretJSON != "" {
		files = append(files, ignitionFile(cfg.DataPath+"/auth.json", 0600, cfg.PullSecretJSON))
	}

	return files
}

func ignitionFile(path string, mode int, content string) map[string]interface{} {
	return map[string]interface{}{
		"path":      path,
		"mode":      int64(mode),
		"overwrite": true,
		"contents": map[string]interface{}{
			"source": "data:text/plain;base64," + base64.StdEncoding.EncodeToString([]byte(content)),
		},
	}
}

func buildQuayConfig(cfg *Config) string {
	return fmt.Sprintf(`AUTHENTICATION_TYPE: Database
BUILDLOGS_REDIS:
  host: localhost
  password: mirror-registry-redis-pass
  port: 6379
DATABASE_SECRET_KEY: bWlycm9yLXJlZ2lzdHJ5LWRiLXNlY3JldC1rZXk=
DB_URI: sqlite:////sqlite/quay_sqlite.db
DEFAULT_TAG_EXPIRATION: 2w
DISTRIBUTED_STORAGE_DEFAULT_LOCATIONS: []
DISTRIBUTED_STORAGE_PREFERENCE:
  - default
DISTRIBUTED_STORAGE_CONFIG:
  default:
    - LocalStorage
    - storage_path: /datastorage
FEATURE_ANONYMOUS_ACCESS: true
FEATURE_BUILD_SUPPORT: false
FEATURE_DIRECT_LOGIN: true
FEATURE_MAILING: false
FEATURE_SECURITY_SCANNER: false
FEATURE_USER_CREATION: true
FEATURE_USER_INITIALIZE: true
CREATE_NAMESPACE_ON_PUSH: true
PREFERRED_URL_SCHEME: https
REGISTRY_TITLE: Mirror Registry
SECRET_KEY: bWlycm9yLXJlZ2lzdHJ5LXNlY3JldC1rZXk=
SERVER_HOSTNAME: localhost:%d
SETUP_COMPLETE: true
SUPER_USERS:
  - admin
USER_EVENTS_REDIS:
  host: localhost
  password: mirror-registry-redis-pass
  port: 6379
USE_CDN: false
WORKER_COUNT_UNSUPPORTED_MINIMUM: 1
WORKER_COUNT: 1
`, cfg.Port)
}

func buildInitScript(cfg *Config) string {
	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail

QUAY_CONFIG_DIR="%[1]s/quay-config"
CA_DIR="%[1]s/ca"
mkdir -p "${QUAY_CONFIG_DIR}"

# Generate server TLS cert signed by shared CA if not present
if [ ! -f "${QUAY_CONFIG_DIR}/ssl.cert" ]; then
    echo "Generating CA-signed TLS certificate..."
    NODE_HOSTNAME=$(hostname)
    NODE_IP=$(ip -4 addr show scope global | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -1)

    cat > "${QUAY_CONFIG_DIR}/openssl.cnf" <<EOF
[req]
default_bits = 4096
default_md = sha256
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = ${NODE_HOSTNAME}
[v3_req]
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${NODE_HOSTNAME}
DNS.2 = localhost
IP.1 = ${NODE_IP}
IP.2 = 127.0.0.1
EOF

    openssl genrsa -out "${QUAY_CONFIG_DIR}/ssl.key" 4096

    openssl req -new -key "${QUAY_CONFIG_DIR}/ssl.key" \
        -out "${QUAY_CONFIG_DIR}/ssl.csr" \
        -config "${QUAY_CONFIG_DIR}/openssl.cnf"

    openssl x509 -req -days 3650 \
        -in "${QUAY_CONFIG_DIR}/ssl.csr" \
        -CA "${CA_DIR}/ca.crt" \
        -CAkey "${CA_DIR}/ca.key" \
        -CAcreateserial \
        -out "${QUAY_CONFIG_DIR}/ssl.cert" \
        -extensions v3_req \
        -extfile "${QUAY_CONFIG_DIR}/openssl.cnf"

    chmod 644 "${QUAY_CONFIG_DIR}/ssl.cert" "${QUAY_CONFIG_DIR}/ssl.key"

    cp "${CA_DIR}/ca.crt" /etc/pki/ca-trust/source/anchors/mirror-registry-ca.crt
    update-ca-trust

    rm -f "${QUAY_CONFIG_DIR}/ssl.csr"
    echo "TLS certificate generated for ${NODE_HOSTNAME} / ${NODE_IP} (signed by shared CA)"
fi

# Create podman volumes if they don't exist
podman volume exists sqlite-storage 2>/dev/null || podman volume create sqlite-storage
podman volume exists quay-storage 2>/dev/null || podman volume create quay-storage

# Create redis password secret if it doesn't exist
if ! podman secret inspect redis_pass &>/dev/null; then
    echo -n "mirror-registry-redis-pass" | podman secret create redis_pass -
fi

echo "Mirror registry init complete"
`, cfg.DataPath)
}

func buildSystemdUnits(cfg *Config) []interface{} {
	authfileArg := ""
	if cfg.PullSecretJSON != "" {
		authfileArg = fmt.Sprintf(" --authfile %s/auth.json", cfg.DataPath)
	}

	units := []interface{}{
		systemdUnit("mirror-registry-init.service", true,
			buildInitUnit(cfg.DataPath)),
		systemdUnit("mirror-registry-pod.service", true,
			buildPodUnit(cfg, authfileArg)),
		systemdUnit("mirror-registry-redis.service", true,
			buildRedisUnit(cfg, authfileArg)),
		systemdUnit("mirror-registry-app.service", true,
			buildAppUnit(cfg, authfileArg)),
		systemdUnit("mirror-registry-admin-init.service", true,
			buildAdminInitUnit()),
	}

	return units
}

func systemdUnit(name string, enabled bool, contents string) map[string]interface{} {
	return map[string]interface{}{
		"name":     name,
		"enabled":  enabled,
		"contents": contents,
	}
}

func buildInitUnit(dataPath string) string {
	return fmt.Sprintf(`[Unit]
Description=Initialize Mirror Registry (TLS, volumes, secrets)
After=network-online.target
Wants=network-online.target
Before=mirror-registry-pod.service
ConditionPathExists=!%s/quay-config/ssl.cert

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/local/bin/mirror-registry-init.sh

[Install]
WantedBy=multi-user.target
`, dataPath)
}

func buildPodUnit(cfg *Config, authfileArg string) string {
	return fmt.Sprintf(`[Unit]
Description=Mirror Registry Pod
After=network-online.target mirror-registry-init.service
Wants=network-online.target
Requires=mirror-registry-init.service
Before=kubelet.service crio.service

[Service]
Type=simple
RemainAfterExit=yes
TimeoutStartSec=5m
ExecStartPre=-/usr/bin/podman pod stop quay-pod
ExecStartPre=-/usr/bin/podman pod rm -f quay-pod
ExecStartPre=/usr/bin/podman pull --tls-verify=false%[1]s %[2]s
ExecStart=/usr/bin/podman pod create \
    --name quay-pod \
    --infra-image %[2]s \
    --publish %[3]d:%[3]d \
    --replace
ExecStop=-/usr/bin/podman pod stop quay-pod -t 10
ExecStopPost=-/usr/bin/podman pod rm -f quay-pod
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
`, authfileArg, cfg.PauseImage, cfg.Port)
}

func buildRedisUnit(cfg *Config, authfileArg string) string {
	return fmt.Sprintf(`[Unit]
Description=Mirror Registry Redis
After=network-online.target mirror-registry-pod.service
Wants=network-online.target
Requires=mirror-registry-pod.service
Before=mirror-registry-app.service kubelet.service crio.service

[Service]
Type=simple
TimeoutStartSec=5m
ExecStartPre=-/usr/bin/podman stop quay-redis
ExecStartPre=-/usr/bin/podman rm -f quay-redis
ExecStartPre=/usr/bin/podman pull --tls-verify=false%[1]s %[2]s
ExecStart=/usr/bin/podman run \
    --name quay-redis \
    --pod=quay-pod \
    --secret=redis_pass,type=env,target=REDIS_PASSWORD \
    --replace \
    %[2]s
ExecStop=-/usr/bin/podman stop quay-redis -t 10
ExecStopPost=-/usr/bin/podman rm -f quay-redis
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
`, authfileArg, cfg.RedisImage)
}

func buildAppUnit(cfg *Config, authfileArg string) string {
	return fmt.Sprintf(`[Unit]
Description=Mirror Registry Quay
After=network-online.target mirror-registry-redis.service
Wants=network-online.target
Requires=mirror-registry-pod.service mirror-registry-redis.service
Before=kubelet.service crio.service

[Service]
Type=simple
TimeoutStartSec=5m
ExecStartPre=-/usr/bin/podman stop quay-app
ExecStartPre=-/usr/bin/podman rm -f quay-app
ExecStartPre=/usr/bin/podman pull --tls-verify=false%[1]s %[2]s
ExecStart=/usr/bin/podman run \
    --name quay-app \
    -v %[3]s/quay-config:/quay-registry/conf/stack:Z \
    -v sqlite-storage:/sqlite:Z,U \
    -v quay-storage:/datastorage:Z \
    --pod=quay-pod \
    --replace \
    -e WORKER_COUNT_UNSUPPORTED_MINIMUM=1 \
    -e WORKER_COUNT=1 \
    -e PYTHONUSERBASE_SITE_PACKAGE=/opt/app-root/lib/python3.12/site-packages \
    %[2]s registry
ExecStop=-/usr/bin/podman stop quay-app -t 10
ExecStopPost=-/usr/bin/podman rm -f quay-app
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
`, authfileArg, cfg.QuayImage, cfg.DataPath)
}

func buildAdminInitScript(cfg *Config) string {
	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail

MAX_WAIT=300
ELAPSED=0

echo "Waiting for local Quay registry to be healthy..."
while ! curl -sk --max-time 5 "https://localhost:%[1]d/api/v1/discovery" > /dev/null 2>&1; do
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    if [ $ELAPSED -ge $MAX_WAIT ]; then
        echo "ERROR: Quay did not become healthy in ${MAX_WAIT}s"
        exit 1
    fi
done

echo "Quay is healthy, initializing admin user..."
RESPONSE=$(curl -sk -o /dev/null -w "%%{http_code}" \
    -X POST "https://localhost:%[1]d/api/v1/user/initialize" \
    -H 'Content-Type: application/json' \
    -d '{"username": "admin", "password": "mirror-registry-admin", "email": "admin@localhost", "access_token": true}')

if [ "$RESPONSE" = "200" ]; then
    echo "Admin user initialized successfully"
else
    echo "Admin user already initialized (HTTP ${RESPONSE})"
fi
`, cfg.Port)
}

func buildAdminInitUnit() string {
	return `[Unit]
Description=Initialize Mirror Registry Admin User
After=mirror-registry-app.service
Requires=mirror-registry-app.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/local/bin/mirror-registry-admin-init.sh

[Install]
WantedBy=multi-user.target
`
}
