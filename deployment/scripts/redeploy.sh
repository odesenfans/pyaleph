#! /bin/bash
# Redeploys a PyAleph node using the specified Docker Compose file.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

function help() {
  echo "$0 --file <docker-compose-file> [--key-file <private-key-file>]"
  echo "(Re)starts an instance of PyAleph node."
  echo "  --file, -f"
  echo "      Path to the Docker Compose file to use."
  echo "  --key-file, -k"
  echo "      Path to the private key file, if you do not already have a 'keys' directory."
}

DOCKER_COMPOSE_FILE=""
KEY_FILE=""
RESET_DATA=""

while test $# -gt 0; do
  case "$1" in
  -h | --help)
    help
    ;;
  -f | --file)
    shift
    DOCKER_COMPOSE_FILE=$(realpath "$1")
    ;;
  -k | --key-file)
    shift
    KEY_FILE=$1
    ;;
  --reset-data)
    shift
    RESET_DATA=y
    ;;
  esac
  shift
done

if [ -z "${DOCKER_COMPOSE_FILE}" ]; then
  echo >&2 "No Docker Compose file specified."
  help
  exit 1
fi

DOCKER_COMPOSE_DIR=$(dirname "${DOCKER_COMPOSE_FILE}")

# Check if the config file is present.
CONFIG_FILE="${DOCKER_COMPOSE_DIR}/config.yml"
if [ ! -f "${CONFIG_FILE}" ]; then
  >&2 echo "Config file (${CONFIG_FILE}) not found."
  exit 1
fi

# Check that the keys exist, create them or migrate them if needed.
KEYS_DIR="${DOCKER_COMPOSE_DIR}/keys"
if [ ! -d "${KEYS_DIR}" ]; then
  if [ -z "${KEY_FILE}" ]; then
    echo "No private key file specified."
    echo "Generating a new keys directory in ${KEYS_DIR}..."
    # Get the Docker image from the compose file to generate the keys
    PYALEPH_IMAGE=$(python3 "${SCRIPT_DIR}/image_tag_from_compose.py" -f "${DOCKER_COMPOSE_FILE}" pyaleph)
    docker run --rm -ti --user root -v "${KEYS_DIR}":/opt/pyaleph/keys "${PYALEPH_IMAGE}" chown aleph:aleph /opt/pyaleph/keys
    docker run \
      --rm \
      -v "${KEYS_DIR}":/opt/pyaleph/keys \
      "${PYALEPH_IMAGE}" \
      pyaleph --gen-keys

    echo "Keys generated in ${KEYS_DIR}."
  else
    echo "Migrating private key ${KEY_FILE} to  ${KEYS_DIR}..."
    python3 "${SCRIPT_DIR}/migrate_key_file_for_p2pd.py" --key-file "${KEY_FILE}" --output-dir "${KEYS_DIR}"

    echo "Migrated the keys to the new format in ${KEYS_DIR}."
  fi
else
  if [ ! -f "${KEYS_DIR}/node-secret.key" ] || [ ! -f "${KEYS_DIR}/serialized-node-secret.key" ]; then
    echo >&2 "The keys directory (${KEYS_DIR}) does not contain the expected keys."
    exit 1
  fi
  echo "Keys found in ${KEYS_DIR}."
fi

# Stop the node
docker-compose -f "${DOCKER_COMPOSE_FILE}" down

# Reset data if requested
if [ "${RESET_DATA}" == "y" ]; then
  docker volume rm docker-monitoring_pyaleph-ipfs
  docker volume rm docker-monitoring_pyaleph-mongodb
fi

# Restart the node
docker-compose -f "${DOCKER_COMPOSE_FILE}" up -d
