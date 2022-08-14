FROM ubuntu:20.04

ARG USR
ENV USR $USR

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN useradd -m -s /bin/bash $USR

RUN apt-get update -y
RUN apt-get install -y clang clang-tidy libc++-dev libc++abi-dev libtool cmake git
