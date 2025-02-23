FROM debian:bullseye

ARG DEBIAN_FRONTEND=noninteractive

COPY /test/docker/install-clang.sh /
RUN /install-clang.sh 19 clang-tidy-19
RUN apt-get update -y \
     && apt-get install libboost-filesystem-dev=1.74.* libboost-dev=1.74.* -y --no-install-recommends \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build

RUN cmake \
    -DCMAKE_BUILD_TYPE=Debug \
    -DPISA_BUILD_TOOLS=ON \
    -DPISA_CLANG_TIDY_EXECUTABLE='clang-tidy' \
    -DPISA_ENABLE_CLANG_TIDY=ON \
    -DCMAKE_TOOLCHAIN_FILE=clang.cmake \
    -DPISA_SYSTEM_BOOST=ON \
    ..\
    && cmake --build . --config Debug -- -j 3
