FROM debian:bullseye

ARG CXX_STANDARD=17
ARG USE_SANITIZERS=OFF
ARG DEBIAN_FRONTEND=noninteractive

COPY /test/docker/install-clang.sh /
RUN /install-clang.sh 15

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build

RUN cmake \
    -DCMAKE_BUILD_TYPE='Debug' \
    -DPISA_BUILD_TOOLS='OFF' \
    -DPISA_ENABLE_BENCHMARKING='OFF' \
    -DCMAKE_TOOLCHAIN_FILE='clang.cmake' \
    "-DUSE_SANITIZERS=$USE_SANITIZERS" \
    "-DCMAKE_CXX_STANDARD=$CXX_STANDARD" \
    .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
