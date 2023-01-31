FROM silkeh/clang:12

ARG CXX_STANDARD=17

ENV TOOLCHAIN="-DCMAKE_TOOLCHAIN_FILE=clang.cmake"
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    cmake=3.18.* \
    libtool=2.4.* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake \
    -DCMAKE_BUILD_TYPE='Debug' \
    -DPISA_BUILD_TOOLS='OFF' \
    -DPISA_ENABLE_BENCHMARKING='OFF' \
    -DCMAKE_TOOLCHAIN_FILE='clang.cmake' \
    .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
