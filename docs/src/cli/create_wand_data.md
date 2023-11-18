# create_wand_data

## Usage

```
<!-- cmdrun ../../../build/bin/create_wand_data --help -->
```

## Description

Creates additional data needed for certain query algorithms.
See [`"WAND" Data`](../guide/wand_data.md) for more details.

Refer to [`queries`](queries.html) for details about scoring functions.

## Blocks

Each posting list is divided into blocks, and each block gets a
precomputed max score. These blocks can be either of equal size
throughout the index, defined by `--block-size`, or variable based on
the lambda parameter `--lambda`. [TODO: Explanation needed]
