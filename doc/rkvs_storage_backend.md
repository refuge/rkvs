

# Module rkvs_storage_backend #
* [Data Types](#types)

__This module defines the `rkvs_storage_backend` behaviour.__<br /> Required callback functions: `open/2`, `close/1`, `destroy/1`, `contains/2`, `get/2`, `put/3`, `clear/2`, `write_batch/2`, `scan/4`, `clear_range/4`, `fold_keys/4`, `fold/4`, `is_empty/1`.

<a name="types"></a>

## Data Types ##




### <a name="type-fold_options">fold_options()</a> ###



<pre><code>
fold_options() = [{start_key, binary()} | {end_key, binary()} | {gt, binary()} | {gte, binary()} | {lt, binary()} | {lte, binary()} | {max, integer()}]
</code></pre>





### <a name="type-kv">kv()</a> ###



<pre><code>
kv() = {Key::binary(), Value::binary()}
</code></pre>





### <a name="type-kvs">kvs()</a> ###



<pre><code>
kvs() = [<a href="#type-kv">kv()</a>]
</code></pre>





### <a name="type-write_ops">write_ops()</a> ###



<pre><code>
write_ops() = [{put, Key::binary(), Value::any()} | {delete, Key::binary()} | clear]
</code></pre>


