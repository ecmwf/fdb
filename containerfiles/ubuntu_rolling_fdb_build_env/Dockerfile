FROM ubuntu:rolling

ARG CMAKE_VERSION=4.2.0
ARG NINJA_VERSION=1.13.1

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/opt/venv/bin:$PATH"

RUN apt-get update \
    && apt-get install -y \
    build-essential \
    git \
    curl \
    wget \
    patchelf \
    python3-full \
    python3-venv \
    python3-dev \
    gfortran\
    ninja-build \
    bison \
    flex \
    zstd \
    libeigen3-dev \
    libncurses-dev \
    libqhull-dev \
    libcurl4-openssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/* \
    && rm -rf /var/tmp/*
RUN mkdir -p /opt/venv \
    && python3 -m venv /opt/venv \
    && . /opt/venv/bin/activate \
    && pip install --no-cache-dir \
        "zarr>=3,<4" \
        numpy \
        pytest \
        pytest-asyncio \
        eccodes \
        pyfdb \
        build setuptools
RUN wget -q https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz \
    && tar -zxf cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz -C /usr/local --strip-components=1 \
    && rm -rf cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz
RUN wget -q https://github.com/ninja-build/ninja/archive/v${NINJA_VERSION}.tar.gz \
    && tar -zxf v${NINJA_VERSION}.tar.gz \
    && cd ninja-${NINJA_VERSION} \
    && python3 configure.py --bootstrap \
    && cp ninja /usr/local/bin/ \
    && cd .. \
    && rm -rf ninja-${NINJA_VERSION} v${NINJA_VERSION}.tar.gz
ENTRYPOINT ["/bin/bash"]
