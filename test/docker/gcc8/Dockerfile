FROM gcc:8

ENV PISA_SRC="/pisa"
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

WORKDIR $PISA_SRC

RUN apt-get update -y
RUN apt-get install -y cmake
