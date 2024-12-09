FROM gcc:13

ARG USE_SANITIZERS=OFF

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York
ENV COLLECTION_PATH=/opt/disk45
ENV WORKDIR=/opt/workdir

COPY . /pisa
RUN ./pisa/test/docker/install-cmake.sh

RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake \
    "-DCMAKE_BUILD_TYPE=Release" \
    "-DPISA_BUILD_TOOLS=ON" \
    "-DPISA_ENABLE_TESTING=OFF" \
    "-DPISA_ENABLE_BENCHMARKING=OFF" \
    "-DUSE_SANITIZERS=$USE_SANITIZERS" \
    .. \
    && cmake --build . --config Debug -- -j 4

COPY ./test/docker/benchmark/run.sh /pisa/build
COPY ./test/docker/benchmark/build.sh /pisa/build
COPY ./test/docker/benchmark/evaluate.sh /pisa/build
COPY ./test/docker/benchmark/bench.sh /pisa/build
COPY ./test/docker/benchmark/setup.sh /pisa/build
COPY ./test/docker/benchmark/expected-eval.txt /pisa/build
COPY ./test/docker/benchmark/encodings.sh /pisa/build

CMD ["bash", "/pisa/test/docker/benchmark/run.sh"]
