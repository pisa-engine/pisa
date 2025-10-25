# sample_inverted_index

## Usage

```
<!-- cmdrun ../../../build/bin/sample_inverted_index --help -->
```

## Description
Creates a smaller inverted index from an existing one by sampling postings or documents. The purpose of this tool is to reduce time and space requirements while preserving the main statistical properties of the original collection, making it useful for faster experiments and debugging.

### Sampling strategy (`-t, --type`)

- `random_postings`: keep random occurrences per posting list (not whole posting lists).
- `random_docids`: keep all postings belonging to a random subset of documents.

## Examples

### Keep ~25% of postings
```bash
sample_inverted_index -c path/to/inverted -o path/to/inverted.sampled -r 0.25 -t random_postings
```

### Keep ~25% of the documents
```bash
sample_inverted_index -c path/to/inverted -o path/to/inverted.sampled -r 0.25 -t random_docids
```
