.. pisa documentation master file, created by
   sphinx-quickstart on Mon Feb 18 02:32:49 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to PISA
===============

Performant Indexes and Search for Academia

Description
------------

PISA is a text search engine able to run on large-scale collections of documents. It allows researchers to experiment with state-of-the-art techniques, allowing an ideal environment for rapid development.

Some features of PISA are listed below:

* Written in C++ for performance;
* Indexing & Parsing & Sharding capabilities;
* Many index compression methods implemented;
* Many query processing algorithms implemented;
* Implementation of document reordering;
* Free and open-source with permissive license;

.. note:: PISA is still in its **initial release** and many new features are going to come in the next versions. Contributions are also welcome!


.. toctree::
   :maxdepth: 4
   :caption: Contents:

   getting_started
   indexing_pipeline
   parsing
   inverting
   sharding
   compress_index
   query_index
   document_reordering
   threshold_estimation
