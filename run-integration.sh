#!/usr/bin/env bash

# docker compose down -v
# docker compose up -d
# docker compose logs -f mindsdb

set -euo pipefail

# Ruta base del repo (ajusta si corres desde otra carpeta)
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$REPO_DIR"

# Levantar servicios de docker-compose
echo "[INFO] Levantando servicios de docker-compose..."
docker compose up -d

# Variables de entorno requeridas por los tests
export CLIENT_ID="968599897015-7c6bal6mjlo9df32mkfn5rob2m43igui.apps.googleusercontent.com"
export MINDSDB_HOST=127.0.0.1
export MINDSDB_PORT=47335   # host port mapeado en docker-compose.yml
export MINDSDB_DATABASE=testdb
export MINDSDB_USER=mindsdb
export MINDSDB_PASS=none

export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3307
export MYSQL_DATABASE=testdb
export MYSQL_USER=mindsdb
export MYSQL_PASS=mindsdb
export MINDSDB_MYSQL_USER=testuser
export MINDSDB_MYSQL_PASS=testpass

# Mostrar configuración
cat <<EOF
[INFO] Variables de entorno exportadas:
  CLIENT_ID=$CLIENT_ID
  MINDSDB_HOST=$MINDSDB_HOST
  MINDSDB_PORT=$MINDSDB_PORT
  MINDSDB_DATABASE=$MINDSDB_DATABASE
  MINDSDB_USER=$MINDSDB_USER
  MINDSDB_PASS=$MINDSDB_PASS

  MYSQL_HOST=$MYSQL_HOST
  MYSQL_PORT=$MYSQL_PORT
  MYSQL_DATABASE=$MYSQL_DATABASE
  MYSQL_USER=$MYSQL_USER
  MYSQL_PASS=$MYSQL_PASS
EOF

# Ejecutar test de integración
echo "[INFO] Ejecutando integration tests de MindsDB..."
go test ./tests/mindsdb -run TestMindsDBToolEndpoints -v