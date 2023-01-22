FROM ubuntu:22.04 as base

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -y upgrade && apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:deadsnakes/ppa

# Runtime + build packages
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
     git \
     libgmp-dev \
     libsecp256k1-dev \
     python3.11

FROM base as builder

# Build-only packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    pkg-config \
    python3.11-dev \
    python3.11-venv \
    software-properties-common

# Install Rust to build Python packages
RUN curl https://sh.rustup.rs > rustup-installer.sh
RUN sh rustup-installer.sh -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Some packages (py-ed25519-bindings, required by substrate-interface) need the nightly
# Rust toolchain to be built at this time
RUN rustup default nightly

# Create virtualenv
RUN python3.11 -m venv /opt/venv

# Install pip
ENV PIP_NO_CACHE_DIR yes
RUN /opt/venv/bin/python3.11 -m pip install --upgrade pip wheel
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /opt/pyaleph
COPY alembic.ini setup.cfg setup.py ./
COPY deployment/scripts ./deployment/scripts
COPY .git ./.git
COPY src ./src
RUN ls /opt/pyaleph
RUN pip install -e .


FROM base

RUN useradd -s /bin/bash aleph

COPY --from=builder --chown=aleph /opt/venv /opt/venv
COPY --from=builder --chown=aleph /opt/pyaleph /opt/pyaleph

ENV PATH="/opt/venv/bin:${PATH}"
WORKDIR /opt/pyaleph
USER aleph
ENTRYPOINT ["bash", "deployment/scripts/run_aleph_ccn.sh"]
