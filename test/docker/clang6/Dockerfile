FROM silkeh/clang:6

ENV PISA_SRC="/pisa"
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

ENV TOOLCHAIN="-DCMAKE_TOOLCHAIN_FILE=clang.cmake"

WORKDIR $PISA_SRC

RUN apt-get update -y
RUN apt-get install -y cmake libtool
