# PISA Controller

NOTE: this is an experimental feature. It is currently only meant to
work with the current version of PISA. Some of the concepts used here
will eventually be migrated to PISA proper. In the meantime, this will
make it easy to build and maintain indices.

# Minimal Examples

## Build from IR-datasets

```sh
pisactl index ir-datasets wikir/en1k --content-fields text -o /path/to/index --encoding block_simdbp
```

## Build from CIFF

```sh
pisactl index ciff -i /path/to/ciff -o /path/to/index --encoding block_simdbp
```

## Build by piping a collection

```sh
cat plaintext.collection | pisactl index stdin --format plaintext -o /path/to/index --encoding block_simdbp
```

## Build multiple compressed indexes

You can build multiple indexes for a collection and use different
parameters. For example, you can build both quantized and non-quantized
indexes:

```sh
pisactl index ciff -i /path/to/ciff -o /path/to/index --encoding block_simdbp
pisactl index pisa -w /tmp/wikir --alias quantized --encoding block_simdbp --quantize 8
```

The first index will be available under alias `default`. For any
subsequent index, you must provide the alias explicitly. If you wish to
define alias for the first index, you can do that as well with the
`--alias` option.

You can use the alias later to query a specific index version. See
examples below.

## Query

Get results in TREC format.

```sh
# single query
echo -n 'query' | pisactl query -w /tmp/wikir
# queries in file
cat queries.txt | pisactl query -w /tmp/wikir
```

Run benchmarks:

```sh
# single query
echo -n 'query' | pisactl query -w /tmp/wikir --benchmark
# queries in file
cat queries.txt | pisactl query -w /tmp/wikir --benchmark
```

### Query by alias

You can specify the alias of the index you want to query. If not
provided, `default` is used. If no `default` alias exists (e.g., because
you provided a different alias for the first index), then you must
provide the alias at query time.

```sh
echo -n 'query' | pisactl query -w /tmp/wikir --alias default
echo -n 'query' | pisactl query -w /tmp/wikir --alias quantized
```
