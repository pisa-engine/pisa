# Regression test for Robust04

This tutorial explains how to run regression tests on the
[Robust04][r04] collection.

[r04]: https://trec.nist.gov/data/t13_robust.html

## Requirements

Due to the collection's license, we cannot distribute it, and thus you
will need to obtain it separately to run this test. See
https://trec.nist.gov/data/cd45/index.html for more information.

## Docker Image

In the repository, we provide a Docker image definition under
`test/docker/benchmark` to make reproducing results easier. First, to
build the image, run the following command from the repository's root
directory:

```
docker build -t pisa-bench -f- .. < test/docker/benchmark/Dockerfile
```

You can use any name instead of `pisa-bench` but if you use a different
one, make sure to substitute it in any subsequent command.

Building the image may take a while because the tools will be compiled.

Once the image is built, you can run it with:

```
podman run \
    -v path/to/disk45:/opt/disk45:ro \
    -v your/workdir:/opt/workdir \
    --rm pisa-bench
```

Replace `path/to/disk45` with your local path to the Robust04 disk 4 and
5 directory, and `your/workdir` with the path to a local working
directory, where artifacts will be written.

By default, the script will build all indices and run evaluation. If you
want to inspect the image or run some custom commands, you can execute
the image interactively:

```
podman run \
    -v $HOME/data/disk45:/opt/disk45:ro \
    -v $HOME/workdir:/opt/workdir \
    --rm -it pisa-bench /bin/bash
```
