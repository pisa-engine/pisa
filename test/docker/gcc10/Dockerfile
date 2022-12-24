FROM gcc:10

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    cmake=3.13.* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake "-DCMAKE_BUILD_TYPE=Debug" "-DPISA_BUILD_TOOLS=OFF" .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
