#!/usr/bin/env bats

set +x

sorted_values=$(cat <<HERE
adipiscing
amet
arcu
bibendum
consectetuer
convallis
dictum
donec
elementum
elit
erat
fames
fringilla
id
integer
ipsum
lorem
morbi
odor
platea
quam
ridiculus
ultricies
HERE
)

unsorted_values=$(cat <<HERE
arcu
adipiscing
ipsum
fringilla
consectetuer
amet
integer
lorem
donec
morbi
elit
convallis
dictum
fames
id
bibendum
odor
erat
platea
quam
ultricies
elementum
ridiculus
HERE
)

assert_eq () {
    if [ "$1" != "$2" ]; then
        echo "failed assertion: $1 == $2"
        exit 1
    fi
}

count_expected_bytes () {
    local bytes
    local newlines
    bytes=$(wc -m < "$1")
    newlines=$(wc -l < "$1")
    words=$(wc -w < "$1")
    echo "16 + ($words + 1) * 4 + $bytes - $newlines" | bc
}

@test "build sorted input" {
    workdir=$(mktemp -d)
    input_file="$workdir/input"
    lt="$workdir/lt"

    echo "$workdir"
    printf "%s\n" "$sorted_values" > "$input_file"

    # build
    lookup-table build -o "$lt" < "$input_file"

    # verify size
    count_expected_bytes "$input_file" > "$workdir/expected_bytes"
    wc -c > "$workdir/actual_bytes" < "$lt"
    diff "$workdir/expected_bytes" "$workdir/actual_bytes"

    # print by index
    assert_eq "$(lookup-table print "$lt" --at 0)" adipiscing
    assert_eq "$(lookup-table print "$lt" --at 10)" erat
    assert_eq "$(lookup-table print "$lt" --at 15)" ipsum
    assert_eq "$(lookup-table print "$lt" --at 16)" lorem
    assert_eq "$(lookup-table print "$lt" --at 22)" ultricies
    # out of bounds exits with a failure exit code and prints out error
    run lookup-table print "$lt" --at 23
    (( status != 0 ))
    assert_eq "${lines[0]}" 'error: position 23 in a table of size 23 is out of bounds'

    # find
    assert_eq "$(lookup-table find "$lt" adipiscing)" 0
    assert_eq "$(lookup-table find "$lt" erat)" 10
    assert_eq "$(lookup-table find "$lt" ipsum)" 15
    assert_eq "$(lookup-table find "$lt" lorem)" 16
    assert_eq "$(lookup-table find "$lt" ultricies)" 22
    # no element found
    run lookup-table find "$lt" zonk
    (( status != 0 ))
    assert_eq "${lines[0]}" "error: value 'zonk' not found"

    # print

    lookup-table print "$lt" > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --from 0 --to 22 > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --from 0 --count 23 > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --from 5 --to 17 > "$workdir/printed"
    diff "$workdir/printed" <(head -18 "$workdir/input" | tail -13)

    lookup-table print "$lt" --from 5 --count 13 > "$workdir/printed"
    diff "$workdir/printed" <(head -18 "$workdir/input" | tail -13)

    lookup-table print "$lt" --to 22 > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --count 23 > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --to 10 > "$workdir/printed"
    diff "$workdir/printed" <(head -11 "$workdir/input")

    lookup-table print "$lt" --count 10 > "$workdir/printed"
    diff "$workdir/printed" <(head -10 "$workdir/input")
}

@test "build unsorted input" {
    workdir=$(mktemp -d)
    input_file="$workdir/input"
    lt="$workdir/lt"

    echo "$workdir"
    printf "%s\n" "$unsorted_values" > "$input_file"

    # build
    lookup-table build -o "$lt" < "$input_file"

    # verify size
    count_expected_bytes "$input_file" > "$workdir/expected_bytes"
    wc -c > "$workdir/actual_bytes" < "$lt"
    diff "$workdir/expected_bytes" "$workdir/actual_bytes"

    # print by index
    assert_eq "$(lookup-table print "$lt" --at 0)" arcu
    assert_eq "$(lookup-table print "$lt" --at 10)" elit
    assert_eq "$(lookup-table print "$lt" --at 15)" bibendum
    assert_eq "$(lookup-table print "$lt" --at 16)" odor
    assert_eq "$(lookup-table print "$lt" --at 22)" ridiculus
    # out of bounds exits with a failure exit code and prints out error
    run lookup-table print "$lt" --at 23
    (( status != 0 ))
    assert_eq "${lines[0]}" 'error: position 23 in a table of size 23 is out of bounds'

    # find
    assert_eq "$(lookup-table find "$lt" arcu)" 0
    assert_eq "$(lookup-table find "$lt" elit)" 10
    assert_eq "$(lookup-table find "$lt" bibendum)" 15
    assert_eq "$(lookup-table find "$lt" odor)" 16
    assert_eq "$(lookup-table find "$lt" ridiculus)" 22
    # no element found
    run lookup-table find "$lt" zonk
    (( status != 0 ))
    assert_eq "${lines[0]}" "error: value 'zonk' not found"

    # print

    lookup-table print "$lt" > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --from 0 --to 22 > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --from 0 --count 23 > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --from 5 --to 17 > "$workdir/printed"
    diff "$workdir/printed" <(head -18 "$workdir/input" | tail -13)

    lookup-table print "$lt" --from 5 --count 13 > "$workdir/printed"
    diff "$workdir/printed" <(head -18 "$workdir/input" | tail -13)

    lookup-table print "$lt" --to 22 > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --count 23 > "$workdir/printed"
    diff "$workdir/printed" "$workdir/input"

    lookup-table print "$lt" --to 10 > "$workdir/printed"
    diff "$workdir/printed" <(head -11 "$workdir/input")

    lookup-table print "$lt" --count 10 > "$workdir/printed"
    diff "$workdir/printed" <(head -10 "$workdir/input")
}
