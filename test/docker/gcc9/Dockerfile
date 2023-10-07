FROM gcc:9

ARG CXX_STANDARD=17
ARG USE_SANITIZERS=OFF

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

COPY . /pisa
RUN ./pisa/test/docker/install-cmake.sh

RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake \
    "-DCMAKE_BUILD_TYPE=Debug" \
    "-DPISA_BUILD_TOOLS=OFF" \
    "-DPISA_ENABLE_BENCHMARKING=OFF" \
    "-DPISA_SANITIZERS=$USE_SANITIZERS" \
    "-DCMAKE_CXX_STANDARD=$CXX_STANDARD" \
    .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
