

# rkvs - Simple Erlang Key/Values interface #

Copyright (c) 2014-2015 Benoît Chesneau.

__Version:__ 0.2.0

rkvs is a simple Key-Value database interface. It offers for now a frontend to
the following K/V storages: ets, leveldb, rocksdb, hanoidb, bitcask.

## Example of usage

### Enable a backend

To enable one of the backend, add one of the following line to your rebar or
make the depency availavle in ERL_LIBS:

```
    {leveldb, ".*", {git, "https://github.com/basho/eleveldb.git",
                     {tag, "2.1.0"}}},

    {erocksdb, ".*", {git, "https://github.com/leo-project/erocksdb.git",
                      {branch, "develop"}}},

    {hanoidb, ".*", {git, "https://github.com/krestenkrab/hanoidb.git",
                     "4e82d5f81ab087f038bfd13354ff48ee9113f459"}},

    {bitcask, ".*", {git, "https://github.com/basho/bitcask.git",
                     {tag, "2.0.0"}}},
```

### Opecreate a database:

```
Name = "mydb",
{ok, Engine} = rkvs:open(Name, [{backend, rkvs_leveldb}]).
```

2 backends are available:

- `rkvs_ets`: ETS backend
- `rkvs_leveldb`: LevelDB backend using [eleveldb](https://github.com/basho/eleveldb) from Basho.

### Store a values

Storing a value associated to a key using `rkvs:put/3`:

```
Key = <<"a">>,
Value = 1,
ok =  rkvs:put(Engine, Key, Value).
```

### Retrieve a value

Use the `rkvs:get/2` function to retrieve a value.

```
Value = rkvs:get(Engine, Key).
```

Value should be 1

> Note: you can use `rkvs:contains/2`.

### Delete a value

Use `rkvs:clear/2` to delete a value:

```
ok = rkvs:clear(Engine, Key).
```

### Store and write multiples values in one pass:

Using `rkvs:write_batch/2` you can write and delete multiple values in one
pass:

```
ok =  rkvs:write_batch(Engine, [{put, <<"a">>, 1},
                                {put, <<"b">>, 2},
                                {put, <<"c">>, 3}]),

ok =  rkvs:write_batch(Engine, [{put, <<"d">>, 4},
                                {delete, <<"b">>},
                                {put, <<"e">>, 5}]).
```

### Retrieve multiple values in a range:

Use `rkvs:scan/4` to retrieve multiples K/Vs as a list:

```
Result = rkvs:scan(Engine, first, nil, 0).
```

Result should be `[{<<"a">>,1},{<<"c">>,3},{<<"d">>,4},{<<"e">>,5}]`

> Use `rkvs:fold/5` to pass a function to keys in a range instead of retrieving
> all the values as a list. `rkvs:clear_range/4` can be used to clear all the
> values in a range.

### Close a storage

Close a storage using `rkvs:close/1`:

```
rkvs:close(Engine)
```

> You can use `rkvs:destroy/1` to close and delete the full storage

## Ownership and License

The contributors are listed in AUTHORS. This project uses the MPL v2
license, see LICENSE.

rkvs uses the [C4.1 (Collective Code Construction
Contract)](http://rfc.zeromq.org/spec:22) process for contributions.

## Development

Under C4.1 process, you are more than welcome to help us by:

* join the discussion over anything from design to code style try out
* and [submit issue reports](https://github.com/refuge/rkvs/issues/new)
* or feature requests pick a task in
* [issues](https://github.com/refuge/rkvs/issues) and get it done fork
* the repository and have your own fixes send us pull requests and even
* star this project ^_^

To  run the test suite:

```
make test
```



## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="http://github.com/refuge/rkvs/blob/master/doc/rkvs.md" class="module">rkvs</a></td></tr>
<tr><td><a href="http://github.com/refuge/rkvs/blob/master/doc/rkvs_bitcask.md" class="module">rkvs_bitcask</a></td></tr>
<tr><td><a href="http://github.com/refuge/rkvs/blob/master/doc/rkvs_ets.md" class="module">rkvs_ets</a></td></tr>
<tr><td><a href="http://github.com/refuge/rkvs/blob/master/doc/rkvs_hanoidb.md" class="module">rkvs_hanoidb</a></td></tr>
<tr><td><a href="http://github.com/refuge/rkvs/blob/master/doc/rkvs_leveldb.md" class="module">rkvs_leveldb</a></td></tr>
<tr><td><a href="http://github.com/refuge/rkvs/blob/master/doc/rkvs_rocksdb.md" class="module">rkvs_rocksdb</a></td></tr>
<tr><td><a href="http://github.com/refuge/rkvs/blob/master/doc/rkvs_storage_backend.md" class="module">rkvs_storage_backend</a></td></tr>
<tr><td><a href="http://github.com/refuge/rkvs/blob/master/doc/rkvs_util.md" class="module">rkvs_util</a></td></tr></table>

