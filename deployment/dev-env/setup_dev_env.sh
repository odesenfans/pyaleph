#! /bin/bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )
ROOT_DIR="${SCRIPT_DIR}/../.."

cd "${ROOT_DIR}"

sudo apt update && sudo apt install \
    libbz2-dev \
    libgflags-dev \
    libgmp-dev \
    libleveldb-dev \
    liblz4-dev \
    librocksdb-dev \
    libsecp256k1-dev \
    libsnappy-dev \
    libssl-dev \
    libyaml-dev \
    pkg-config \
    python3 \
    python3-pip \
    zlib1g-dev

pip3 install virtualenv

if [ ! -d ${ROOT_DIR}/venv ]; then
    python3 -m virtualenv venv
fi

source venv/bin/activate
pip install -e .

