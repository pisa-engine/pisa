FROM gcc:13

ARG CXX_STANDARD=17

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
    | gpg --dearmor | tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null

RUN echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" \
    | tee /etc/apt/sources.list.d/oneAPI.list

RUN apt-get -y update \
    && apt-get install -y \
    libbenchmark-dev \
    libboost-dev \
    intel-oneapi-tbb-devel-2021.10.0 \
    libcli11-dev

COPY . /pisa
RUN ./pisa/test/docker/install-cmake.sh

RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN . /opt/intel/oneapi/setvars.sh && cmake \
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
