# create_wand_data

## Usage

```
<!-- cmdrun ../../../build/bin/create_wand_data --help -->
```

## Description

Creates additional data needed for certain query algorithms.

Algorithms such as WAND and MaxScore (among others) need more data than
available in posting lists alone. This includes max scores for each
term, as well as max scores for ranges of posting lists that can be used
as skip lists.

Refer to [`queries`](queries.html) for details about scoring functions.

## Blocks

Each posting list is divided into blocks, and each block gets a
precomputed max score. These blocks can be either of equal size
throughout the index, defined by `--block-size`, or variable based on
the lambda parameter `--lambda`. [TODO: Explanation needed]
