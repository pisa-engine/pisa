set -e

log () {
    (( silent )) || {
        [[ -n "$encoding" ]] && printf '[%s] ' "$encoding" 1>&2
        printf "$@" 1>&2
    }
}

queries () {
    ./bin/queries \
        -e "$encoding" \
        -w "$bmw" \
        -i "$index" \
        -k "$k" \
        --algorithm "$algorithm" \
        --scorer "$scorer" \
        --terms "$terms" \
        -q "$queries" \
        --stemmer porter2 \
        --silent
}

run () {
    log 'Starting a run for encoding: %s\n' "$encoding"
    local output="$output_basename/$run_name-$encoding.json"
    local index="$index_basename.$encoding"
    log 'Removing old file: %s\n' "$output"
    rm -f "$output"
    for (( i=1; i<=repeat; i++ )); do
        log '%d/%d\n' "$i" "$repeat"
        queries >> "$output"
    done
}

help() {
    echo "Usage: bench
        -c | --config CONFIG
        -n | --run-name NAME
        [ --dry-run ]
        [ -s | --silent ]
        [ -h | --help  ]"
    exit 2
}

main () {
    SHORT=c:,n:,s,h
    LONG=config:,name:,silent,dry-run,help
    OPTS=$(getopt -a -n bench --options $SHORT --longoptions $LONG -- "$@")
    eval set -- "$OPTS"

    local dry_run=0
    local config=''
    local run_name=''
    local silent=0

    while :
    do
        case "$1" in
            --dry-run )
                dry_run=1
                shift
                ;;
            -s | --silent )
                silent=1
                shift
                ;;
            -h | --help) help ;;
            -c | --config)
                config="$2"
                shift 2
                ;;
            -n | --run-name)
                run_name="$2"
                shift 2
                ;;
            --) shift
                break ;;
            *) echo "Unexpected option: $1"; help ;;
        esac
    done

    [[ "$config" == '' ]] && {
        printf 'You must define config.\n' 1>&2
        help
    }
    [[ "$run_name" == '' ]] && {
        printf 'You must define run name.\n' 1>&2
        help
    }
    source "$config"

    log 'Creating output directory: %s\n' "$output_basename"
    mkdir -p "$output_basename"
    local bmw="$index_basename.bmw"
    local terms="$index_basename.termlex"
    for encoding in "${encodings[@]}"; do
        run
    done
}

main "$@"
