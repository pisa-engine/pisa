FROM silkeh/clang:11

ENV TOOLCHAIN="-DCMAKE_TOOLCHAIN_FILE=clang.cmake"
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    cmake=3.18.* \
    libtool=2.4.* \
    clang-tidy=1:11.* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake -DCMAKE_BUILD_TYPE=Debug -DPISA_BUILD_TOOLS=ON -DPISA_CLANG_TIDY_EXECUTABLE='clang-tidy' \
    -DPISA_ENABLE_CLANG_TIDY=ON -DCMAKE_TOOLCHAIN_FILE=clang.cmake .. \
    && cmake --build . --config Debug -- -j 3
