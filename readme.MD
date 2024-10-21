## HASH_DS_CHUNKIFY

[![Run Hash Script](https://github.com/uaineteine/hash_ds_chunkify/actions/workflows/run_chunkfy_test.yaml/badge.svg)](https://github.com/uaineteine/hash_ds_chunkify/actions/workflows/run_chunkfy_test.yaml)

A extension to HASH_DS, a tool that will hash dataframes on a list of variables and a key. The python script it multi-threaded to optimise the execution to the number of user cores that are available.

A configuration template is used to start the process and datasets are automatically chunked into smaller datasets or parts

## Example execute

One can select the truncation limit for this output

```ps
python -m hash_ds_chunkify ../hash_ds/testdata/data1.psv usernames,Header2
```

## Package Requirements

In extension of HASH_DS requirements

* configparser

## GNU Licence
