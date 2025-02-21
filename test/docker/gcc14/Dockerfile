FROM gcc:14

ARG USE_SANITIZERS=OFF
ARG BUILD_TYPE=Debug

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

COPY . /pisa
RUN ./pisa/test/docker/install-cmake.sh

RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake \
    "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" \
    "-DPISA_BUILD_TOOLS=OFF" \
    "-DPISA_ENABLE_BENCHMARKING=OFF" \
    "-DPISA_SANITIZERS=$USE_SANITIZERS" \
    .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
