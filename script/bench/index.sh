set -e

log () {
    (( silent )) || {
        [[ -n "$encoding" ]] && printf '[%s] ' "$encoding" 1>&2
        printf "$@" 1>&2
    }
}

wand_data () {
    log 'create_wand_data -s %s -c %s -o %s -b %d\n' \
        "$scorer" "$index" "$index.bmw" "$block_size"
    ./bin/create_wand_data \
        -s "$scorer" \
        -c "$index_basename" \
        -o "$index_basename.bmw" \
        -b "$block_size"
}

compress () {
    ./bin/compress_inverted_index \
        -e "$encoding" \
        -c "$index_basename" \
        -o "$index_basename.$encoding" \
        --check
}

help() {
    echo "Usage: bench
        -c | --config CONFIG
        [ -s | --silent ]
        [ -h | --help  ]"
    exit 2
}

main () {
    SHORT=c:,n:,s,h
    LONG=config:,name:,silent,dry-run,help
    OPTS=$(getopt -a -n bench --options $SHORT --longoptions $LONG -- "$@")
    eval set -- "$OPTS"

    local config=''

    while :
    do
        case "$1" in
            -s | --silent )
                silent=1
                shift
                ;;
            -h | --help) help ;;
            -c | --config)
                config="$2"
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
    source "$config"

    wand_data
    for encoding in "${encodings[@]}"; do
        compress
    done
}

main "$@"
