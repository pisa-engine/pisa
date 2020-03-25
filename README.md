<p align="center"><img src="https://pisa-engine.github.io/images/logo250.png" width="250px"></p>

# PISA: Performant Indexes and Search for Academia v0.8.1

[![Build Status](https://travis-ci.com/pisa-engine/pisa.svg?branch=master)](https://travis-ci.com/pisa-engine/pisa)
[![codecov](https://codecov.io/gh/pisa-engine/pisa/branch/master/graph/badge.svg)](https://codecov.io/gh/pisa-engine/pisa)
[![Documentation Status](https://readthedocs.org/projects/pisa/badge/?version=latest)](https://pisa.readthedocs.io/en/latest/?badge=latest)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/83cbd7128c084994a87fb8394bd91a16)](https://www.codacy.com/app/amallia/pisa?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=pisa-engine/pisa&amp;utm_campaign=Badge_Grade)
[![GitHub issues](https://img.shields.io/github/issues/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/issues)
[![GitHub forks](https://img.shields.io/github/forks/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/network)
[![GitHub stars](https://img.shields.io/github/stars/pisa-engine/pisa.svg)](https://github.com/pisa-engine/pisa/stargazers)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/pisa-engine/pisa/pulls)
[![DOI](https://zenodo.org/badge/150449350.svg)](https://zenodo.org/badge/latestdoi/150449350)

## Join us on Slack
Get in touch via Slack: [![Slack](https://img.shields.io/badge/slack-join-blue.svg)](https://join.slack.com/t/pisa-engine/shared_invite/enQtNjM1NTk3NzIyMjE0LTQ3ZjI1MmU2ZjAyODE4YjNiNTY5YWYzMjg5Njc5ZDM5MzhhZDBiMGE5MTFhMTViN2ZjNzg0OTkzMDAwMDg3YTE)


## Description

PISA is a text search engine able to run on large-scale collections of documents. It allows researchers to experiment with state-of-the-art techniques, allowing an ideal environment for rapid development.

Some features of PISA are listed below:

* Written in C++ for performance;
* Indexing & Parsing & Sharding capabilities;
* Many index compression methods implemented;
* Many query processing algorithms implemented;
* Implementation of document reordering;
* Free and open-source with permissive license;

The best way to get started is by reading the [official documentation](http://pisa.readthedocs.io).

If you want to get involved with PISA, please check out our [Contributing](https://github.com/pisa-engine/pisa/blob/master/.github/CONTRIBUTING.md) page.

## Reference

Reference to cite when you use PISA in a research paper:

```
@inproceedings{MSMS2019,
  author    = {Antonio Mallia and Michal Siedlaczek and Joel Mackenzie and Torsten Suel},
  title     = {{PISA:} Performant Indexes and Search for Academia},
  booktitle = {Proceedings of the Open-Source {IR} Replicability Challenge co-located
               with 42nd International {ACM} {SIGIR} Conference on Research and Development
               in Information Retrieval, OSIRRC@SIGIR 2019, Paris, France, July 25,
               2019.},
  pages     = {50--56},
  year      = {2019},
  url       = {http://ceur-ws.org/Vol-2409/docker08.pdf}
}
```

#### Credits
PISA is a fork of the [ds2i](https://github.com/ot/ds2i/) project started by [Giuseppe Ottaviano](https://github.com/ot).
