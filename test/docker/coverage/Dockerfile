FROM gcc:13

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    cmake=3.25.* \
    lcov=1.16* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /pisa
COPY test/docker/coverage/cov.sh /pisa/build/cov.sh
WORKDIR /pisa/build
RUN cmake -DCMAKE_BUILD_TYPE=Debug -DPISA_BUILD_TOOLS=OFF -DENABLE_COVERAGE=ON .. \
    && cmake --build . --config Debug -- -j 4

CMD ["bash", "cov.sh"]
