FROM ubuntu:rolling

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/opt/ecmwf/bin:/opt/venv/bin:$PATH"
ENV FDB5_HOME=/opt/ecmwf/fdb

RUN apt-get update \
    && apt-get install -y \
    python3 \
	python3-venv \
    libncurses6 \
    libqhull8.0 \
    zstd \
    libaec0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/* \
    && rm -rf /var/tmp/*

# Expect the context to be set to a build path after CPACK ran
COPY ["fdb-*.tar.zst", "z3fdb-*.whl", "/tmp/"]

RUN mkdir -p /opt/venv \
    && python3 -m venv /opt/venv \
    && . /opt/venv/bin/activate \
    && pip install --no-cache-dir "zarr>=3,<4" numpy pytest pytest-asyncio eccodes pyfdb build setuptools $(ls /tmp/z3fdb*.whl)

RUN mkdir -p /opt/ecmwf \
    && tar xvf $(ls /tmp/fdb-*.tar.zst) -C /opt/ecmwf/ \
    && cd /opt/ecmwf/ \
    && ln -s $(ls /opt/ecmwf | grep fdb) fdb

# CMD ["-c", "\"import z3fdb; print('success')\""]

ENTRYPOINT ["/opt/venv/bin/python"]
