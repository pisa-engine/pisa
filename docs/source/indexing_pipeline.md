# Indexing Pipeline

This section is an overview of how to take a collection
to a state in which it can be queried.
This process is intentionally broken down into several steps,
with a bunch of independent tools performing different tasks.
This is because we want the flexibility of experimenting with each
individual step without recomputing the entire pipeline.

![Indexing Pipeline](img/pipeline.jpg)
