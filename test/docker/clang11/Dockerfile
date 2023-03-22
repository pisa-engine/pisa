FROM debian:bullseye

ARG CXX_STANDARD=17
ARG DEBIAN_FRONTEND=noninteractive

COPY . /pisa
RUN ./pisa/test/docker/install-clang.sh 11

RUN mkdir /pisa/build
WORKDIR /pisa/build

RUN cmake \
    -DCMAKE_BUILD_TYPE='Debug' \
    -DPISA_BUILD_TOOLS='OFF' \
    -DPISA_ENABLE_BENCHMARKING='OFF' \
    .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
