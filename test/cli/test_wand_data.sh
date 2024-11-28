#!/usr/bin/env bats

set +x

queries=$(cat <<HERE
301:International Organized Crime
302:Poliomyelitis and Post-Polio
303:Hubble Telescope Achievements
304:Endangered Species (Mammals)
305:Most Dangerous Vehicles
306:African Civilian Deaths
307:New Hydroelectric Projects
308:Implant Dentistry
309:Rap and Crime
310:Radio Waves and Brain Cancer
HERE
)

eval_queries () {
    wand_data_path="$1"
    echo "$queries" | evaluate_queries \
        -e block_simdbp \
        -a block_max_wand \
        -i "simdbp" \
        -w "$wand_data_path" \
        -F lowercase -F porter2 \
        --terms "fwd.termlex" \
        --documents "fwd.doclex" \
        -k 10 \
        --scorer bm25 \
        -q "topics.robust2004.title"
}

@test "Build uncompressed wand data with fixed block size" {
    output_dir=$(mktemp -d)
    create_wand_data \
        --scorer bm25 \
        --collection "./inv" \
        --output "./wdata.raw" \
        --block-size 32
    eval_queries "bm25.bmw" > "$output_dir/expected"
    eval_queries "wdata.raw" > "$output_dir/actual"
    diff "$output_dir/expected" "$output_dir/actual"
}

@test "Build uncompressed wand data with variable block size" {
    output_dir=$(mktemp -d)
    create_wand_data \
        --scorer bm25 \
        --collection "./inv" \
        --output "./wdata.raw" \
        -l 4
    eval_queries "bm25.bmw" > "$output_dir/expected"
    eval_queries "wdata.raw" > "$output_dir/actual"
    diff "$output_dir/expected" "$output_dir/actual"
}

@test "Build compressed wand data with fixed block size" {
    output_dir=$(mktemp -d)
    create_wand_data \
        --scorer bm25 \
        --collection "./inv" \
        --output "./wdata.compressed" \
        --block-size 32 \
        --compress \
        --quantize 8
    eval_queries "bm25.bmw" > "$output_dir/expected"
    eval_queries "wdata.raw" > "$output_dir/actual"
    diff "$output_dir/expected" "$output_dir/actual"
}

@test "--compress cannot be without --quantize" {
    # Compressed wand data inherently quantizes the scores,
    # even if it supports non-quantized scores through
    # projection to floats at query time.
    run create_wand_data \
        --scorer bm25 \
        --collection "./inv" \
        --output "./wdata.quantized" \
        --block-size 32 \
        --compress
    (( status != 0 ))
}

@test "--block-size and --lambda are mutually exclusive" {
    run create_wand_data \
        --scorer bm25 \
        --collection "./inv" \
        --output "./wdata.quantized" \
        --block-size 32 \
        --lambda 32
    (( status != 0 ))
}
