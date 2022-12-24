FROM silkeh/clang:15-bullseye

ENV TOOLCHAIN="-DCMAKE_TOOLCHAIN_FILE=clang.cmake"
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    cmake=3.18.* \
    libtool=2.4.* \
    libc++-15-dev=1:15.* \
    libc++abi-15-dev=1:15.* \
    git=1:2.* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake -DCMAKE_BUILD_TYPE='Debug' -DPISA_BUILD_TOOLS='OFF' -DCMAKE_TOOLCHAIN_FILE='clang.cmake' .. \
    && cmake --build . --config Debug -- -j 4

CMD ["ctest", "-VV", "-j", "4"]
