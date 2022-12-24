FROM gcc:12 AS builder

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    cmake=3.18.* \
    libtool=2.4.* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /pisa
WORKDIR /pisa

RUN mkdir build
WORKDIR /pisa/build
RUN cmake -DCMAKE_BUILD_TYPE=Release -DPISA_BUILD_TOOLS=ON -DPISA_ENABLE_TESTING=OFF ..
RUN cmake --build . --config Release -- -j 4

CMD ["ctest", "-VV", "-j", "4"]

FROM debian:bullseye

COPY --from=builder /pisa/build/bin/* /usr/bin/
