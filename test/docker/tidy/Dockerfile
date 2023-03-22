FROM debian:bullseye

ARG DEBIAN_FRONTEND=noninteractive

COPY /test/docker/install-clang.sh /
RUN /install-clang.sh 15 clang-tidy-15

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build

RUN cmake -DCMAKE_BUILD_TYPE=Debug -DPISA_BUILD_TOOLS=ON -DPISA_CLANG_TIDY_EXECUTABLE='clang-tidy' \
    -DPISA_ENABLE_CLANG_TIDY=ON -DCMAKE_TOOLCHAIN_FILE=clang.cmake .. \
    && cmake --build . --config Debug -- -j 3
