FROM gcc:13

ARG CXX_STANDARD=17

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

COPY . /pisa
RUN ./pisa/test/docker/install-cmake.sh

RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake \
    "-DCMAKE_BUILD_TYPE=Debug" \
    "-DPISA_BUILD_TOOLS=ON" \
    "-DPISA_ENABLE_BENCHMARKING=ON" \
    "-DPISA_SYSTEM_GOOGLE_BENCHMARK=ON" \
    "-DPISA_SYSTEM_ONETBB=ON" \
    "-DPISA_SYSTEM_BOOST=ON" \
    "-DPISA_SYSTEM_CLI11=ON" \
    "-DCMAKE_CXX_STANDARD=$CXX_STANDARD" \
    .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
