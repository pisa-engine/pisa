#!/usr/bin/env bats

PISA_BIN="bin"
export PATH="$PISA_BIN:$PATH"

. "$BATS_TEST_DIRNAME/common.sh"

function setup {
    write_lines "$BATS_TMPDIR/queries.txt" "brooklyn tea house" "labradoodle" 'Tell your dog I said "hi"'
}

@test "Compute single intersection" {
    result=$(compute_intersection --stemmer porter2 --terms ./fwd.termlex -q $BATS_TMPDIR/queries.txt \
                -e block_simdbp -i ./simdbp -w ./bm25.bmw)
    expected='{"intersections":[{"length":2,"mask":7,"max_score":11.045351028442383}],"query":{"query":"brooklyn tea house","term_ids":[6535,29194,15462],"terms":["brooklyn","tea","hous"]}}
{"intersections":[{"length":0,"mask":0,"max_score":0.0}],"query":{"query":"labradoodle","term_ids":[],"terms":[]}}
{"intersections":[{"length":1,"mask":63,"max_score":10.947325706481934}],"query":{"query":"Tell your dog I said \"hi\"","term_ids":[29287,32766,10396,15670,26032,15114],"terms":["tell","your","dog","i","said","hi"]}}'
    echo $result > "$BATS_TMPDIR/test.log"
    [[ "$result" = "$expected" ]]
}

@test "Compute combinations with --mtc 1 (effectively single terms)" {
    result=$(compute_intersection --stemmer porter2 --terms ./fwd.termlex -q $BATS_TMPDIR/queries.txt \
                -e block_simdbp -i ./simdbp -w ./bm25.bmw --combinations --mtc 1)
    expected='{"intersections":[{"length":10,"mask":1,"max_score":6.536393642425537},{"length":20,"mask":2,"max_score":6.352736949920654},{"length":82,"mask":4,"max_score":3.8619942665100098}],"query":{"query":"brooklyn tea house","term_ids":[6535,29194,15462],"terms":["brooklyn","tea","hous"]}}
{"intersections":[],"query":{"query":"labradoodle","term_ids":[],"terms":[]}}
{"intersections":[{"length":156,"mask":1,"max_score":2.819181442260742},{"length":493,"mask":2,"max_score":0.05130492523312569},{"length":168,"mask":4,"max_score":2.7635655403137207},{"length":408,"mask":8,"max_score":0.6954182386398315},{"length":103,"mask":16,"max_score":3.5857112407684326},{"length":33,"mask":32,"max_score":6.272759914398193}],"query":{"query":"Tell your dog I said \"hi\"","term_ids":[29287,32766,10396,15670,26032,15114],"terms":["tell","your","dog","i","said","hi"]}}'
    echo $result > "$BATS_TMPDIR/test.log"
    [[ "$result" = "$expected" ]]
}

@test "Compute combinations with --mtc 1 (single terms and pairs)" {
    result=$(compute_intersection --stemmer porter2 --terms ./fwd.termlex -q $BATS_TMPDIR/queries.txt \
                -e block_simdbp -i ./simdbp -w ./bm25.bmw --combinations --mtc 2)
    expected='{"intersections":[{"length":10,"mask":1,"max_score":6.536393642425537},{"length":20,"mask":2,"max_score":6.352736949920654},{"length":2,"mask":3,"max_score":8.58621883392334},{"length":82,"mask":4,"max_score":3.8619942665100098},{"length":2,"mask":5,"max_score":7.098772048950195},{"length":5,"mask":6,"max_score":8.58519458770752}],"query":{"query":"brooklyn tea house","term_ids":[6535,29194,15462],"terms":["brooklyn","tea","hous"]}}
{"intersections":[],"query":{"query":"labradoodle","term_ids":[],"terms":[]}}
{"intersections":[{"length":156,"mask":1,"max_score":2.819181442260742},{"length":493,"mask":2,"max_score":0.05130492523312569},{"length":82,"mask":3,"max_score":2.850811243057251},{"length":168,"mask":4,"max_score":2.7635655403137207},{"length":13,"mask":5,"max_score":4.625458240509033},{"length":114,"mask":6,"max_score":2.7950499057769775},{"length":408,"mask":8,"max_score":0.6954182386398315},{"length":64,"mask":9,"max_score":3.0511868000030518},{"length":219,"mask":10,"max_score":0.7440643310546875},{"length":64,"mask":12,"max_score":2.8497495651245117},{"length":103,"mask":16,"max_score":3.5857112407684326},{"length":32,"mask":17,"max_score":5.245534420013428},{"length":87,"mask":18,"max_score":3.626940965652466},{"length":26,"mask":20,"max_score":4.820082187652588},{"length":89,"mask":24,"max_score":4.2715864181518555},{"length":33,"mask":32,"max_score":6.272759914398193},{"length":16,"mask":33,"max_score":6.646618843078613},{"length":26,"mask":34,"max_score":6.29996919631958},{"length":3,"mask":36,"max_score":6.239107131958008},{"length":28,"mask":40,"max_score":6.949945449829102},{"length":15,"mask":48,"max_score":8.103652000427246}],"query":{"query":"Tell your dog I said \"hi\"","term_ids":[29287,32766,10396,15670,26032,15114],"terms":["tell","your","dog","i","said","hi"]}}'
    echo $result > "$BATS_TMPDIR/test.log"
    [[ "$result" = "$expected" ]]
}
