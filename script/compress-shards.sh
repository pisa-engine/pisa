#!/bin/sh

print_usage()
{
    echo "USAGE:"
    echo "\tcompress-shards <PROGRAM> <INPUT_BASENAME> <OUTPUT_BASENAME> [program flags]"
    exit 1
}

PROGRAM=$1
INPUT_BASENAME=$2
OUTPUT_BASENAME=$3
if [ -z "${PROGRAM}" ]; then print_usage; exit 1; fi;
if [ -z "${INPUT_BASENAME}" ]; then print_usage; exit 1; fi;
if [ -z "${OUTPUT_BASENAME}" ]; then print_usage; exit 1; fi;
shift 3

INPUT_DIR=`dirname $INPUT_BASENAME`

find ${INPUT_DIR} -type f -regextype posix-extended -regex ".*${INPUT_BASENAME}\\.([0-9]){3}\\.docs" | \
while read SHARD_BASENAME; do
    SHARD_BASENAME=`echo ${SHARD_BASENAME} | sed 's/\.docs$//'`
    NUMBER=`echo ${SHARD_BASENAME} | egrep -o '[0-9]{3}'`
    CMD="${PROGRAM} -c ${SHARD_BASENAME} -o "${OUTPUT_BASENAME}.${NUMBER}" $@"
    echo ${CMD}
    ${CMD}
done
