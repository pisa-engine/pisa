#!/bin/sh

print_usage()
{
    echo "USAGE:"
    echo "\tinvert-shards <PROGRAM> <INPUT_BASENAME> <OUTPUT_BASENAME> [program flags]"
    exit 1
}

PROGRAM=$1
INPUT_BASENAME=$2
OUTPUT_BASENAME=$3
if [ -z "${PROGRAM}" ]; then print_usage; exit 1; fi;
if [ -z "${INPUT_BASENAME}" ]; then print_usage; exit 1; fi;
if [ -z "${OUTPUT_BASENAME}" ]; then print_usage; exit 1; fi;
shift 3

find . -type f -regextype posix-extended -regex ".*${INPUT_BASENAME}\\.([0-9]){3}" | \
while read SHARD_BASENAME; do
    #base=`basename ${SHARD_BASENAME}`
    #echo $base
    #NUMBER=`echo ${base} | sed "s/.*-//" | sed "s/\..*//"`
    NUMBER=`echo ${SHARD_BASENAME} | egrep -o '[0-9]{3}'`
    TERM_COUNT=`cat "${SHARD_BASENAME}.terms" | wc -l`
    CMD="${PROGRAM} -i ${SHARD_BASENAME} -o "${OUTPUT_BASENAME}.${NUMBER}" --term-count ${TERM_COUNT} $@"
    echo ${CMD}
    ${CMD}
done
