FROM debian:bullseye

ARG CXX_STANDARD=17
ARG USE_SANITIZERS=OFF
ARG DEBIAN_FRONTEND=noninteractive
ARG BUILD_TYPE=Debug

COPY /test/docker/install-clang.sh /
RUN /install-clang.sh 16
RUN apt-get update -y \
     && apt-get install libboost-filesystem-dev=1.74.* libboost-dev=1.74.* -y --no-install-recommends \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build

RUN cmake \
    "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" \
    -DPISA_BUILD_TOOLS='OFF' \
    -DPISA_ENABLE_BENCHMARKING='OFF' \
    -DPISA_SYSTEM_BOOST='ON' \
    -DCMAKE_TOOLCHAIN_FILE='clang.cmake' \
    "-DPISA_SANITIZERS=$USE_SANITIZERS" \
    "-DCMAKE_CXX_STANDARD=$CXX_STANDARD" \
    .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
