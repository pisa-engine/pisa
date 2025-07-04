FROM debian:bookworm AS build

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get -y update && apt-get install -y gpg-agent wget

RUN wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
    | gpg --dearmor | tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null

RUN echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" \
    | tee /etc/apt/sources.list.d/oneAPI.list

RUN apt-get update && apt-get install -y \
        libboost-dev \
        intel-oneapi-tbb-devel-2021.10.0 \
        libcli11-dev \
        pipx \
        g++ \
        cmake \
        git \
        cargo

COPY . /pisa
WORKDIR /pisa

ENV PATH="/root/.local/bin:$PATH"
RUN pipx install uv

WORKDIR /pisa/pisactl
RUN uv build

RUN apt-get install -y libtool

RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake \
    "-DCMAKE_BUILD_TYPE=Release" \
    "-DPISA_BUILD_TOOLS=ON" \
    "-DPISA_ENABLE_TESTING=OFF" \
    "-DPISA_ENABLE_BENCHMARKING=OFF" \
    "-DPISA_SYSTEM_ONETBB=ON" \
    "-DPISA_SYSTEM_BOOST=ON" \
    "-DPISA_SYSTEM_CLI11=ON" \
    .. \
    && cmake --build . --config Release -- -j 4

WORKDIR /
RUN wget https://github.com/usnistgov/trec_eval/archive/refs/tags/v9.0.8.tar.gz
RUN tar xzvf v9.0.8.tar.gz
WORKDIR /trec_eval-9.0.8
RUN make

FROM rust:1.85 AS ciff_build

WORKDIR /
RUN git clone https://github.com/pisa-engine/ciff.git
WORKDIR /ciff
RUN cargo build --release

FROM debian:bookworm

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get -y update \
    && apt-get install -y libgomp1 curl jq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /pisa/build/bin/* /usr/bin/
COPY --from=build /pisa/pisactl/dist /tmp/dist
COPY --from=ciff_build /ciff/target/release/jsonl2ciff /usr/bin/
COPY --from=ciff_build /ciff/target/release/ciff2pisa /usr/bin/
COPY --from=ciff_build /ciff/target/release/pisa2ciff /usr/bin/
COPY --from=build /opt/intel /opt/intel
COPY --from=build /trec_eval-9.0.8/trec_eval /usr/bin

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN /root/.local/bin/uv venv
RUN /root/.local/bin/uv pip install /tmp/dist/pisactl-0.1.0-py3-none-any.whl
ENV PATH="$HOME/.venv/bin:$PATH"

COPY <<"EOT" /data/queries.txt
southern methodist university
halakha
chief justice of the united states
patsy cline
dierks bentley
goidelic languages
johns hopkins university
kalmar county
manawatu wanganui
bulacan
EOT

RUN echo 'alias pisa="pisactl"' >> ~/.bashrc
