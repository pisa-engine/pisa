FROM gcc:13

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    cmake=3.25.* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /pisa
RUN mkdir /pisa/build
WORKDIR /pisa/build
RUN cmake -DCMAKE_BUILD_TYPE=Debug -DPISA_BUILD_TOOLS=OFF -DPISA_COMPILE_HEADERS=ON ..
